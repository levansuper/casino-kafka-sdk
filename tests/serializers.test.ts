import { describe, it, expect } from 'vitest';
import { JsonSerializer } from '../src/serializers';

describe('JsonSerializer', () => {
  const serializer = new JsonSerializer();

  it('should serialize an object to a Buffer', () => {
    const data = { userId: 'u1', amount: 100 };
    const result = serializer.serialize(data);

    expect(Buffer.isBuffer(result)).toBe(true);
    expect(result.toString()).toBe(JSON.stringify(data));
  });

  it('should deserialize a Buffer back to an object', () => {
    const data = { userId: 'u1', amount: 100 };
    const buffer = Buffer.from(JSON.stringify(data));
    const result = serializer.deserialize(buffer);

    expect(result).toEqual(data);
  });

  it('should roundtrip serialize/deserialize', () => {
    const data = { nested: { key: 'value' }, arr: [1, 2, 3] };
    const result = serializer.deserialize(serializer.serialize(data));

    expect(result).toEqual(data);
  });

  it('should handle primitive values', () => {
    expect(serializer.deserialize(serializer.serialize('hello'))).toBe('hello');
    expect(serializer.deserialize(serializer.serialize(42))).toBe(42);
    expect(serializer.deserialize(serializer.serialize(null))).toBe(null);
    expect(serializer.deserialize(serializer.serialize(true))).toBe(true);
  });

  it('should throw on invalid JSON during deserialize', () => {
    const badBuffer = Buffer.from('not-json');
    expect(() => serializer.deserialize(badBuffer)).toThrow();
  });
});
