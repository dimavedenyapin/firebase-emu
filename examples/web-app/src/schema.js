import {
  MassActionType,
  massActionTypeValidation
} from '@peakflo/peakflo-schema/lib/schemas/massAction/type.massAction';

export { MassActionType };

export function validateMassActionType(value) {
  const result = massActionTypeValidation.validate(value);
  if (result.error) throw result.error;
  return result.value;
}
