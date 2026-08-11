import type { ChatAttachment } from "@t3tools/contracts";
import * as Encoding from "effect/Encoding";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";

import { resolveAttachmentPath } from "../../attachmentStore.ts";

export interface PiImageContent {
  readonly type: "image";
  readonly data: string;
  readonly mimeType: string;
}

export class PiImageAttachmentError extends Schema.TaggedErrorClass<PiImageAttachmentError>()(
  "PiImageAttachmentError",
  {
    attachmentId: Schema.String,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    return `Failed to load Pi image attachment '${this.attachmentId}': ${this.detail}`;
  }
}

export const loadPiImageAttachments = Effect.fn("loadPiImageAttachments")(function* (input: {
  readonly attachments: ReadonlyArray<ChatAttachment> | undefined;
  readonly attachmentsDir: string;
  readonly fileSystem: FileSystem.FileSystem;
}) {
  const images: Array<PiImageContent> = [];
  for (const attachment of input.attachments ?? []) {
    const attachmentPath = resolveAttachmentPath({
      attachmentsDir: input.attachmentsDir,
      attachment,
    });
    if (attachmentPath === null) {
      return yield* new PiImageAttachmentError({
        attachmentId: attachment.id,
        detail: "the attachment path is invalid or unavailable",
      });
    }

    const bytes = yield* input.fileSystem.readFile(attachmentPath).pipe(
      Effect.mapError(
        (cause) =>
          new PiImageAttachmentError({
            attachmentId: attachment.id,
            detail: `could not read '${attachmentPath}'`,
            cause,
          }),
      ),
    );
    images.push({
      type: "image",
      data: Encoding.encodeBase64(bytes),
      mimeType: attachment.mimeType,
    });
  }
  return images;
});
