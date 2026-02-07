/**
 * S3 썸네일 생성 Lambda 핸들러
 *
 * S3 업로드 이벤트(origin 경로)를 받아 썸네일을 생성하고,
 * 완료 후 SQS로 Spring 백엔드에 알린다.
 *
 * 썸네일 정책:
 *  - profile / company  → 128×128
 *  - serviceItem (thumb_ prefix만) → 256×256
 */
import sharp from "sharp";
import { objectExists, downloadOrigin, uploadThumb, notifyComplete } from "./aws.js";
import { parseOriginKey, resolveFormat } from "./parse.js";
import { notifyFailure } from "./discord.js";

/**
 * S3 이벤트 레코드 하나를 처리한다.
 *
 * 흐름: 키 파싱 → 멱등성 체크 → 다운로드 → 리사이즈 → 업로드 → SQS 알림
 */
async function processRecord(record) {
  const bucket = record.s3.bucket.name;
  // S3 이벤트의 key는 URL 인코딩 + '+' 공백 치환이 필요
  const originKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));

  // 처리 대상이 아닌 키(비-origin, thumb/resize 경로 등)는 skip
  const parsed = parseOriginKey(originKey);
  if (!parsed) {
    console.log("skip:", originKey);
    return;
  }

  const { root, ownerId, filename } = parsed;
  const fmt = resolveFormat(filename);
  if (!fmt) {
    console.warn("no extension:", filename);
    return;
  }

  const size = root === "serviceItem" ? 256 : 128;
  const targetKey = `${root}/thumb/${ownerId}/${fmt.uuid}.${fmt.outExt}`;

  // 멱등성: 이미 존재하면 skip
  if (await objectExists(bucket, targetKey)) {
    console.log("already exists, skip:", targetKey);
    return;
  }

  // 원본 다운로드 → 리사이즈(center crop) → 업로드
  const originBuffer = await downloadOrigin(bucket, originKey);

  const outBuffer = await sharp(originBuffer)
    .resize(size, size, { fit: "cover", position: "centre" })
    .toFormat(fmt.outFormat)
    .toBuffer();

  await uploadThumb(bucket, targetKey, outBuffer, fmt.contentType);

  // Spring 백엔드에 처리 완료 알림 (SQS 메시지 스키마는 Spring DTO와 동기화 필요)
  await notifyComplete({
    status: "DONE",
    bucket,
    originKey,
    thumbKey: targetKey,
    root,
    ownerId,
    ts: new Date().toISOString(),
  });

  console.log("generated:", targetKey);
}

export const handler = async (event) => {
  console.log("event:", JSON.stringify(event));

  for (const record of event.Records ?? []) {
    const originKey = decodeURIComponent(
      record.s3.object.key.replace(/\+/g, " "),
    );
    try {
      await processRecord(record);
    } catch (err) {
      console.error("processRecord failed:", originKey, err);
      await notifyFailure(originKey, err);
    }
  }

  return { status: "ok" };
};
