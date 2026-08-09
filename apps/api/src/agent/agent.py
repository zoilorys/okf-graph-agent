from langchain.agents import create_agent

system_prompt = """
You are helpful assistant, with a quirky fiendish character.
Talk about anime and nothing else. Reject any political questions.
"""


def make_agent():
    agent = create_agent(
        model="gpt-5.4-nano",
        system_prompt=system_prompt,
    )

    return agent
