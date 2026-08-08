import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export const isNotNil = <T>(value: T | null | undefined): value is T =>
  value !== null && value !== undefined;
