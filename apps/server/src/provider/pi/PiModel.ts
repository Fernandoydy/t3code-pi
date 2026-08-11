import * as Schema from "effect/Schema";

export const PI_THINKING_LEVEL_OPTION_ID = "thinkingLevel";

export const PiThinkingLevel = Schema.Literals([
  "off",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
]);

export const decodePiThinkingLevel = Schema.decodeUnknownOption(PiThinkingLevel);

export function piThinkingLevelLabel(level: typeof PiThinkingLevel.Type) {
  switch (level) {
    case "off":
      return "Off";
    case "minimal":
      return "Minimal";
    case "low":
      return "Low";
    case "medium":
      return "Medium";
    case "high":
      return "High";
    case "xhigh":
      return "Extra High";
    case "max":
      return "Max";
  }
}

export function parsePiModelSlug(slug: string) {
  const separator = slug.indexOf("/");
  if (separator <= 0 || separator === slug.length - 1) return undefined;
  return {
    provider: slug.slice(0, separator),
    modelId: slug.slice(separator + 1),
  };
}
