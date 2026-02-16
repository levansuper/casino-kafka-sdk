import { Serializer } from './types';

export class JsonSerializer<T = unknown> implements Serializer<T> {
  serialize(data: T): Buffer {
    return Buffer.from(JSON.stringify(data));
  }

  deserialize(buffer: Buffer): T {
    return JSON.parse(buffer.toString()) as T;
  }
}
