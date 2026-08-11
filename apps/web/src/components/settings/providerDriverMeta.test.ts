import { ProviderDriverKind } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { PiAgentIcon } from "../Icons";
import { PROVIDER_ICON_BY_PROVIDER } from "../chat/providerIconUtils";
import { PROVIDER_OPTIONS } from "../../session-logic";
import { DRIVER_OPTION_BY_VALUE } from "./providerDriverMeta";
import { deriveProviderSettingsFields } from "./ProviderSettingsForm";

describe("Pi Agent client registration", () => {
  it("is an active provider with explicit identity and only binary-path settings", () => {
    const pi = ProviderDriverKind.make("piAgent");
    const definition = DRIVER_OPTION_BY_VALUE[pi];

    expect(definition).toMatchObject({
      value: pi,
      label: "Pi Agent",
      icon: PiAgentIcon,
      supportsCustomModels: false,
    });
    expect(deriveProviderSettingsFields(definition!)).toEqual([
      expect.objectContaining({ key: "binaryPath", label: "Binary path" }),
    ]);
    expect(PROVIDER_OPTIONS).toContainEqual(
      expect.objectContaining({ value: pi, label: "Pi Agent", available: true }),
    );
    expect(PROVIDER_ICON_BY_PROVIDER[pi]).toBe(PiAgentIcon);
  });
});
