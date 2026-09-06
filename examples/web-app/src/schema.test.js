import { describe, expect, it } from 'vitest';
import { MassActionType, validateMassActionType } from './schema';

describe('Peakflo schema browser deep import', () => {
  it('accepts a published mass action type', () => {
    expect(validateMassActionType(MassActionType.TRANSACTION_UPDATE)).toBe('TRANSACTION_UPDATE');
  });

  it('rejects values outside the published enum', () => {
    expect(() => validateMassActionType('NOT_A_MASS_ACTION')).toThrow();
  });
});
