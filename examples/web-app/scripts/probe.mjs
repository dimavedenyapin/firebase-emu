import { createServer } from 'vite';
import { chromium } from 'playwright';
const server = await createServer({ server: { host: '127.0.0.1', port: 0 }, logLevel: 'silent' });
let browser;
try {
  await server.listen();
  browser = await chromium.launch({
    headless: true,
    ...(process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE
      ? { executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE }
      : {})
  });
  const page = await browser.newPage();
  page.setDefaultTimeout(10000);
  await page.goto(server.resolvedUrls.local[0]);
  await page.waitForFunction(() => typeof window.runFirebaseProbe === 'function');
  const report = await Promise.race([page.evaluate(() => window.runFirebaseProbe()), new Promise((_, reject) => { const timer = setTimeout(() => reject(new Error('Browser probe exceeded 100 seconds')), 100000); timer.unref(); })]);
  console.log(JSON.stringify(report, null, 2));
  if (process.argv.includes('--strict') && report.results.some(result => result.status !== 'passed')) process.exitCode = 1;
} catch (error) { console.log(JSON.stringify({ sdk: 'firebase/browser', status: 'harness_error', error: error.message })); process.exitCode = 2; }
finally { await browser?.close(); await server.close(); }
