import { describe, expect, it } from 'vitest';
import { ActionType, validateActionType } from './schema';

describe('synthetic input validator', () => {
  it('accepts a known action type', () => {
    expect(validateActionType(ActionType.UPDATE)).toBe('UPDATE');
  });

  it('rejects an unknown action type', () => {
    expect(() => validateActionType('UNKNOWN')).toThrow();
  });
});
