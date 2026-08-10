import { useEffect, useRef, useState, type FormEvent } from 'react';
import { ArrowUp, LoaderCircle, Sparkles } from 'lucide-react';
import ReactMarkdown from 'react-markdown';

type ContentBlock = {
  type: 'text';
  text: string;
};

type MessageStatus = 'pending' | 'streaming' | 'completed';

type Message = {
  id: string;
  role: 'user' | 'assistant';
  content: ContentBlock[];
  created_at: string;
  status: MessageStatus;
};

type ApiMessage = Omit<Message, 'status'>;

type PendingMessage = {
  clientId: string;
  content: string;
  status: 'pending' | 'failed';
};

type MessagesResponse = {
  data: ApiMessage[];
};

type MessageEvent = {
  id: string;
  event_type: 'message.delta' | 'message.created';
  payload: {
    message_id: string;
    role: Message['role'];
    content: ContentBlock[];
    created_at: string;
  };
};

type PresenceEvent = {
  id: string;
  event_type: 'presence';
  payload: {
    typing: boolean;
  };
};

type ConversationEvent = MessageEvent | PresenceEvent;

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '';

const getMessageText = (message: Pick<Message, 'content'>) =>
  message.content
    .filter((block) => block.type === 'text')
    .map((block) => block.text)
    .join('\n');

const appendContent = (current: ContentBlock[], delta: ContentBlock[]) => {
  if (current.length === 0) return delta;

  const next = [...current];
  const lastBlock = next[next.length - 1];
  next[next.length - 1] = {
    ...lastBlock,
    text: lastBlock.text + delta.map((block) => block.text).join(''),
  };
  return next;
};

export const App = () => {
  const [conversationId, setConversationId] = useState<string | null>(() => {
    const id = new URLSearchParams(window.location.search).get('conversation_id');
    return id || null;
  });
  const [isStarting, setIsStarting] = useState(false);
  const [messages, setMessages] = useState<Message[]>([]);
  const [pendingMessages, setPendingMessages] = useState<PendingMessage[]>([]);
  const [draft, setDraft] = useState('');
  const [isSending, setIsSending] = useState(false);
  const [isAgentTyping, setIsAgentTyping] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const bottomRef = useRef<HTMLDivElement>(null);

  const loadMessageHistory = async (id: string) => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/conversations/${id}/messages`);
      if (!response.ok) throw new Error('Could not load messages.');

      const payload = (await response.json()) as MessagesResponse;
      setMessages(payload.data.map((message) => ({ ...message, status: 'completed' })));
      setError(null);
    } catch (requestError) {
      setError(
        requestError instanceof Error
          ? requestError.message
          : 'Could not load messages.',
      );
    }
  };

  useEffect(() => {
    if (!conversationId) return;

    let isCurrentConversation = true;
    let eventSource: EventSource | undefined;
    setIsAgentTyping(false);

    const upsertMessage = (message: Message) => {
      setMessages((current) => {
        const existingIndex = current.findIndex(({ id }) => id === message.id);
        if (existingIndex === -1) return [...current, message];

        const next = [...current];
        next[existingIndex] = message;
        return next;
      });
    };

    const applyEvent = (event: MessageEvent) => {
      const message: Message = {
        id: event.payload.message_id,
        role: event.payload.role,
        content: event.payload.content,
        created_at: event.payload.created_at,
        status: event.event_type === 'message.delta' ? 'streaming' : 'completed',
      };

      if (event.event_type === 'message.created') {
        // A created event is the authoritative version of a message.
        upsertMessage(message);
      } else {
        // Deltas append streamed content to the message, creating it if this is
        // the first chunk received for that message. A completed message is
        // immutable: a late delta can arrive out of order after its final event.
        setMessages((current) => {
          const existingIndex = current.findIndex(({ id }) => id === message.id);
          if (existingIndex === -1) return [...current, message];
          if (current[existingIndex].status === 'completed') return current;

          const next = [...current];
          next[existingIndex] = {
            ...next[existingIndex],
            content: appendContent(next[existingIndex].content, message.content),
            status: 'streaming',
          };
          return next;
        });
      }
    };

    const readEvent = (nativeEvent: globalThis.MessageEvent<string>, eventType?: ConversationEvent['event_type']) => {
      try {
        const data: unknown = JSON.parse(nativeEvent.data);
        const event = (
          data && typeof data === 'object' && 'event_type' in data && 'payload' in data
            ? data
            : { id: nativeEvent.lastEventId, event_type: eventType, payload: data }
        ) as ConversationEvent;

        if (event.event_type === 'presence') {
          if (typeof event.payload?.typing !== 'boolean') return;
          setIsAgentTyping(event.payload.typing);
        } else {
          if (!event.payload) return;
          applyEvent(event);
        }

        setError(null);
      } catch {
        setError('Could not process a conversation update.');
      }
    };

    const connectToEvents = () => {
      eventSource = new EventSource(`${API_BASE_URL}/api/conversations/${conversationId}/events`);
      eventSource.addEventListener('message.created', (event) =>
        readEvent(event as globalThis.MessageEvent<string>, 'message.created'),
      );
      eventSource.addEventListener('message.delta', (event) =>
        readEvent(event as globalThis.MessageEvent<string>, 'message.delta'),
      );
      eventSource.addEventListener('presence', (event) =>
        readEvent(event as globalThis.MessageEvent<string>, 'presence'),
      );
      eventSource.onmessage = (event) => readEvent(event);
      eventSource.onerror = () => {
        setError('Connection to conversation updates was interrupted. Retrying…');
      };
    };

    const initializeConversation = async () => {
      await loadMessageHistory(conversationId);
      if (isCurrentConversation) connectToEvents();
    };

    void initializeConversation();
    return () => {
      isCurrentConversation = false;
      eventSource?.close();
    };
  }, [conversationId]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, pendingMessages]);

  const startConversation = async () => {
    setIsStarting(true);
    setError(null);
    try {
      const response = await fetch(`${API_BASE_URL}/api/conversations`, {
        method: 'POST',
      });
      if (!response.ok) throw new Error('Could not start a conversation.');

      const conversation = (await response.json()) as { id: string };
      setConversationId(conversation.id);
    } catch (requestError) {
      setError(
        requestError instanceof Error
          ? requestError.message
          : 'Could not start a conversation.',
      );
    } finally {
      setIsStarting(false);
    }
  };

  const sendMessage = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const text = draft.trim();
    if (!text || !conversationId || isSending || isAgentTyping) return;

    const clientId = crypto.randomUUID();
    setDraft('');
    setIsSending(true);
    setError(null);
    setPendingMessages((current) => [
      ...current,
      { clientId, content: text, status: 'pending' },
    ]);

    try {
      const response = await fetch(
        `${API_BASE_URL}/api/conversations/${conversationId}/messages`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ content: [{ type: 'text', text }] }),
        },
      );
      if (!response.ok) throw new Error('Could not send your message.');

      const message = (await response.json()) as ApiMessage;
      setPendingMessages((current) =>
        current.filter((pending) => pending.clientId !== clientId),
      );
      setMessages((current) => {
        const existingIndex = current.findIndex(({ id }) => id === message.id);
        const completedMessage: Message = { ...message, status: 'completed' };
        if (existingIndex === -1) return [...current, completedMessage];

        const next = [...current];
        next[existingIndex] = completedMessage;
        return next;
      });
    } catch (requestError) {
      setPendingMessages((current) =>
        current.map((pending) =>
          pending.clientId === clientId ? { ...pending, status: 'failed' } : pending,
        ),
      );
      setError(
        requestError instanceof Error
          ? requestError.message
          : 'Could not send your message.',
      );
    } finally {
      setIsSending(false);
    }
  };

  return (
    <main className="h-screen overflow-hidden bg-[#f8f8f7] px-5 py-8 text-zinc-900 sm:px-8 sm:py-10">
      <section className="mx-auto flex h-[calc(100vh-4rem)] w-full max-w-3xl flex-col rounded-[2rem] border border-zinc-200/80 bg-white shadow-[0_20px_70px_-35px_rgba(24,24,27,0.3)] sm:h-[calc(100vh-5rem)]">
        <header className="flex items-center gap-3 border-b border-zinc-100 px-6 py-5 sm:px-8">
          <div className="grid size-9 place-items-center rounded-xl bg-zinc-900 text-white">
            <Sparkles className="size-4" aria-hidden="true" />
          </div>
          <div>
            <h1 className="font-semibold tracking-tight">Support assistant</h1>
            <p className="text-sm text-zinc-500">How can we help?</p>
          </div>
        </header>

        {!conversationId ? (
          <div className="flex flex-1 flex-col items-center justify-center px-6 text-center">
            <div className="mb-5 grid size-14 place-items-center rounded-2xl bg-zinc-100 text-zinc-700">
              <Sparkles className="size-6" aria-hidden="true" />
            </div>
            <h2 className="text-2xl font-semibold tracking-tight">Let’s get started</h2>
            <p className="mt-2 max-w-sm text-sm leading-6 text-zinc-500">
              Start a conversation and ask anything about your account or service.
            </p>
            <button
              type="button"
              onClick={() => void startConversation()}
              disabled={isStarting}
              className="mt-7 inline-flex h-11 items-center gap-2 rounded-xl bg-zinc-900 px-5 text-sm font-medium text-white transition hover:bg-zinc-700 disabled:cursor-not-allowed disabled:opacity-70"
            >
              {isStarting && <LoaderCircle className="size-4 animate-spin" aria-hidden="true" />}
              Start Conversation
            </button>
          </div>
        ) : (
          <>
            <div className="min-h-0 flex-1 space-y-5 overflow-y-auto px-5 py-7 sm:px-8">
              {messages.length === 0 && pendingMessages.length === 0 && (
                <p className="pt-6 text-center text-sm text-zinc-400">Send a message to begin.</p>
              )}
              {messages.map((message) => (
                <article
                  key={message.id}
                  className={message.role === 'user' ? 'flex justify-end' : 'flex justify-start'}
                >
                  <div
                    className={
                      message.role === 'user'
                        ? 'max-w-[85%] rounded-2xl rounded-br-md bg-zinc-900 px-4 py-3 text-sm leading-6 text-white sm:max-w-[75%]'
                        : 'max-w-[85%] rounded-2xl rounded-bl-md bg-zinc-100 px-4 py-3 text-sm leading-6 text-zinc-800 sm:max-w-[75%]'
                    }
                  >
                    <div
                      className={
                        message.status === 'streaming'
                          ? 'message-markdown message-markdown--streaming'
                          : 'message-markdown'
                      }
                    >
                      <ReactMarkdown>{getMessageText(message)}</ReactMarkdown>
                    </div>
                  </div>
                </article>
              ))}
              {pendingMessages.map((message) => (
                <article key={message.clientId} className="flex justify-end">
                  <div className="max-w-[85%] rounded-2xl rounded-br-md bg-zinc-900 px-4 py-3 text-sm leading-6 text-white opacity-60 sm:max-w-[75%]">
                    {message.content}
                    <span className="mt-1 flex items-center justify-end gap-1 text-xs text-zinc-300">
                      {message.status === 'pending' ? (
                        <><LoaderCircle className="size-3 animate-spin" aria-hidden="true" /> Sending</>
                      ) : (
                        'Not sent'
                      )}
                    </span>
                  </div>
                </article>
              ))}
              {isAgentTyping && (
                <div className="flex justify-start" role="status" aria-live="polite">
                  <div className="flex items-center gap-2 rounded-2xl rounded-bl-md bg-zinc-100 px-4 py-3 text-sm text-zinc-500">
                    <LoaderCircle className="size-4 animate-spin" aria-hidden="true" />
                    Thinking…
                  </div>
                </div>
              )}
              <div ref={bottomRef} />
            </div>

            <form onSubmit={sendMessage} className="border-t border-zinc-100 p-4 sm:p-5">
              <div className="flex items-end gap-3 rounded-2xl border border-zinc-200 bg-white p-2 pl-4 shadow-sm transition focus-within:border-zinc-400 focus-within:ring-4 focus-within:ring-zinc-100">
                <textarea
                  value={draft}
                  onChange={(event) => setDraft(event.target.value)}
                  placeholder="Message support…"
                  rows={1}
                  className="max-h-32 min-h-10 flex-1 resize-none bg-transparent py-2 text-sm leading-6 outline-none placeholder:text-zinc-400"
                  onKeyDown={(event) => {
                    if (event.key === 'Enter' && !event.shiftKey) {
                      event.preventDefault();
                      event.currentTarget.form?.requestSubmit();
                    }
                  }}
                />
                <button
                  type="submit"
                  disabled={!draft.trim() || isSending || isAgentTyping}
                  className="grid size-10 shrink-0 place-items-center rounded-xl bg-zinc-900 text-white transition hover:bg-zinc-700 disabled:cursor-not-allowed disabled:bg-zinc-200 disabled:text-zinc-400"
                  aria-label="Send message"
                >
                  {isSending ? <LoaderCircle className="size-4 animate-spin" /> : <ArrowUp className="size-4" />}
                </button>
              </div>
              <p className="mt-2 text-center text-xs text-zinc-400">Press Enter to send · Shift + Enter for a new line</p>
            </form>
          </>
        )}
      </section>
      {error && <p role="alert" className="mx-auto mt-3 max-w-3xl text-center text-sm text-red-600">{error}</p>}
    </main>
  );
};
