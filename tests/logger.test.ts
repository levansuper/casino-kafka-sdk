import { describe, it, expect, vi } from 'vitest';
import { ConsoleLogger, SdkLogLevel, noopLogger } from '../src/logger';

describe('ConsoleLogger', () => {
  it('should log error at ERROR level', () => {
    const spy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const logger = new ConsoleLogger(SdkLogLevel.ERROR);
    logger.error('test error');
    expect(spy).toHaveBeenCalledOnce();
    spy.mockRestore();
  });

  it('should not log info at ERROR level', () => {
    const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
    const logger = new ConsoleLogger(SdkLogLevel.ERROR);
    logger.info('test info');
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  it('should not log debug at INFO level', () => {
    const spy = vi.spyOn(console, 'debug').mockImplementation(() => {});
    const logger = new ConsoleLogger(SdkLogLevel.INFO);
    logger.debug('test debug');
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  it('should log all levels at DEBUG level', () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    const debugSpy = vi.spyOn(console, 'debug').mockImplementation(() => {});
    const logger = new ConsoleLogger(SdkLogLevel.DEBUG);

    logger.error('e');
    logger.info('i');
    logger.debug('d');

    expect(errorSpy).toHaveBeenCalledOnce();
    expect(logSpy).toHaveBeenCalledOnce();
    expect(debugSpy).toHaveBeenCalledOnce();

    errorSpy.mockRestore();
    logSpy.mockRestore();
    debugSpy.mockRestore();
  });

  it('should log nothing at NONE level', () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    const debugSpy = vi.spyOn(console, 'debug').mockImplementation(() => {});
    const logger = new ConsoleLogger(SdkLogLevel.NONE);

    logger.error('e');
    logger.info('i');
    logger.debug('d');

    expect(errorSpy).not.toHaveBeenCalled();
    expect(logSpy).not.toHaveBeenCalled();
    expect(debugSpy).not.toHaveBeenCalled();

    errorSpy.mockRestore();
    logSpy.mockRestore();
    debugSpy.mockRestore();
  });

  it('should include the prefix and extra data in output', () => {
    const spy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const logger = new ConsoleLogger(SdkLogLevel.ERROR);
    logger.error('connection failed', { broker: 'localhost:9092' });

    expect(spy).toHaveBeenCalledWith(
      '[casino-kafka-sdk] ERROR: connection failed',
      { broker: 'localhost:9092' },
    );
    spy.mockRestore();
  });

  it('should default to INFO level', () => {
    const debugSpy = vi.spyOn(console, 'debug').mockImplementation(() => {});
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    const logger = new ConsoleLogger();

    logger.info('should appear');
    logger.debug('should not appear');

    expect(logSpy).toHaveBeenCalledOnce();
    expect(debugSpy).not.toHaveBeenCalled();

    debugSpy.mockRestore();
    logSpy.mockRestore();
  });
});

describe('noopLogger', () => {
  it('should have all three methods', () => {
    expect(noopLogger.error).toBeTypeOf('function');
    expect(noopLogger.info).toBeTypeOf('function');
    expect(noopLogger.debug).toBeTypeOf('function');
  });

  it('should not throw when called', () => {
    expect(() => noopLogger.error('test')).not.toThrow();
    expect(() => noopLogger.info('test')).not.toThrow();
    expect(() => noopLogger.debug('test')).not.toThrow();
  });
});
