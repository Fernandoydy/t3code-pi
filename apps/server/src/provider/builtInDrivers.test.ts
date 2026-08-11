import { ProviderDriverKind } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { BUILT_IN_DRIVERS } from "./builtInDrivers.ts";

describe("built-in provider drivers", () => {
  it("registers Pi Agent as a multi-instance driver", () => {
    expect(
      BUILT_IN_DRIVERS.find((driver) => driver.driverKind === ProviderDriverKind.make("piAgent")),
    ).toMatchObject({
      driverKind: "piAgent",
      metadata: {
        displayName: "Pi Agent",
        supportsMultipleInstances: true,
      },
    });
  });
});
