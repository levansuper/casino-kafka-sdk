export enum SdkLogLevel {
  NONE = 0,
  ERROR = 1,
  INFO = 2,
  DEBUG = 3,
}

export interface Logger {
  error(message: string, extra?: Record<string, unknown>): void;
  info(message: string, extra?: Record<string, unknown>): void;
  debug(message: string, extra?: Record<string, unknown>): void;
}

export class ConsoleLogger implements Logger {
  private level: SdkLogLevel;

  constructor(level: SdkLogLevel = SdkLogLevel.INFO) {
    this.level = level;
  }

  error(message: string, extra?: Record<string, unknown>): void {
    if (this.level >= SdkLogLevel.ERROR) {
      console.error(`[casino-kafka-sdk] ERROR: ${message}`, extra ?? '');
    }
  }

  info(message: string, extra?: Record<string, unknown>): void {
    if (this.level >= SdkLogLevel.INFO) {
      console.log(`[casino-kafka-sdk] INFO: ${message}`, extra ?? '');
    }
  }

  debug(message: string, extra?: Record<string, unknown>): void {
    if (this.level >= SdkLogLevel.DEBUG) {
      console.debug(`[casino-kafka-sdk] DEBUG: ${message}`, extra ?? '');
    }
  }
}

export const noopLogger: Logger = {
  error() {},
  info() {},
  debug() {},
};
