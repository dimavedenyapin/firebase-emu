export const ActionType = Object.freeze({ UPDATE: 'UPDATE', ARCHIVE: 'ARCHIVE' });

export function validateActionType(value) {
  if (!Object.values(ActionType).includes(value)) throw new Error('Unknown synthetic action type');
  return value;
}
