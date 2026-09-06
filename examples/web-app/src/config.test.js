import { describe, it, expect } from 'vitest';
import { config, endpoint, validateCredentials, validateId, bounded } from './config';
describe('Local configuration', () => {
  it('uses isolated demo project and loopback endpoints', () => { const value = config(); expect(value.projectId).toBe('demo-rust-emu'); expect(value.firestore.port).toBe(8080); });
  it.each(['https://127.0.0.1:8080', 'example.com:8080', 'http://user@localhost:8080', 'http://localhost:8080/path'])('rejects unsafe endpoint %s', value => expect(() => endpoint(value)).toThrow());
  it('rejects a non-demo project', () => expect(() => config({ VITE_FIREBASE_projectId: 'production' })).toThrow());
  it('validates credentials and IDs', () => { expect(() => validateCredentials('bad', 'abcdef')).toThrow(); expect(() => validateCredentials('a@b.test', 'short')).toThrow(); expect(() => validateId('x/y')).toThrow(); expect(validateId('note')).toBe('note'); });
  it('stops waiting for an unavailable service', async () => { await expect(bounded(new Promise(() => {}), 10)).rejects.toThrow('timed out'); });
});
