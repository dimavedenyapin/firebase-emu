import { expect, it, vi } from 'vitest';
import { runProbe } from './probe';
it('reports SDK errors as failures and dependent checks as blocked', async () => {
  const failed = () => Promise.reject(new Error('endpoint missing'));
  const client = { settings: { projectId: 'demo-test' }, signUp: failed, signIn: vi.fn(), signOut: vi.fn().mockResolvedValue(), create: failed, upload: failed };
  const report = await runProbe(client);
  expect(report.results.find(item => item.name === 'schema.massActionType').status).toBe('passed');
  expect(report.results.filter(item => item.status === 'failed').map(item => item.name)).toEqual(['auth.signUp', 'firestore.create', 'firestore.partial-merge', 'firestore.where-order-limit', 'firestore.listener', 'storage.upload']);
  expect(report.results.find(item => item.name === 'auth.signIn').status).toBe('blocked');
  expect(client.signIn).not.toHaveBeenCalled();
  expect(report.results.find(item => item.name === 'firestore.read').status).toBe('blocked');
});
