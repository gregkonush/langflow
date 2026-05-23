import { type Page } from "@playwright/test";
import { test } from "../fixtures";

type TestFunction = (args: { page: Page }) => Promise<void>;
type TestConfig = Parameters<typeof test>[1];

/**
 * Wraps a test function to run it once per v1 ``event_delivery`` mode
 * (streaming / polling / direct) by intercepting ``/api/v1/config``.
 *
 * When the AG-UI flag is on, the v2 workflows endpoint replaces all three v1
 * delivery modes with a single AG-UI SSE path, so the matrix collapses to a
 * single run. Phase 5 deletes this wrapper outright once AG-UI is the only
 * run path.
 *
 * @param title The test title
 * @param config The test configuration (tags, etc)
 * @param testFn The test function to wrap
 */
export function withEventDeliveryModes(
  title: string,
  config: TestConfig,
  testFn: TestFunction,
) {
  if (process.env.LANGFLOW_V2_WORKFLOWS_AGUI_ENABLED === "true") {
    test(title, config, async ({ page }) => {
      await testFn({ page });
    });
    return;
  }

  const eventDeliveryModes = ["streaming", "polling", "direct"] as const;

  for (const eventDelivery of eventDeliveryModes) {
    test(`${title} - ${eventDelivery}`, config, async ({ page }) => {
      // Intercept the config request and modify the event_delivery setting
      await page.route("**/api/v1/config", async (route) => {
        const response = await route.fetch();
        const json = await response.json();
        json.event_delivery = eventDelivery;
        await route.fulfill({ response, json });
      });

      // Run the original test function
      await testFn({ page });
    });
  }
}
