import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import sharp from "sharp";

/**
 * S3: 원본 이미지(origin)를 읽어서
 * - profile/company: 128x128 썸네일을 thumb 경로에 "생성"
 * - serviceItem    : 256x256 리사이즈 이미지를 resize 경로에 "생성"
 *
 * ✅ 중요 정책
 * - origin(원본)은 절대 삭제하지 않는다 (삭제 없이 복사 생성만).
 * - 멱등성(idempotency): 대상 키가 이미 존재하면 재처리하지 않는다.
 * - 루프 방지: lambda가 생성한 thumb/resize 객체 이벤트로 다시 트리거 되지 않도록 제외한다.
 */

const s3 = new S3Client({});
const sqs = new SQSClient({});

/**
 * 썸네일/리사이즈 작업 완료 후 Spring 서버에 알릴 SQS 큐 URL
 * - 환경변수로 주입되어야 함 (Lambda configuration에서 설정)
 */
const QUEUE_URL = process.env.THUMBNAIL_QUEUE_URL;

export const handler = async (event) => {
  /**
   * S3 이벤트는 보통 Records 배열로 들어온다.
   * - 여러 개의 S3 업로드 이벤트가 묶여서 한 번에 들어올 수 있으니 for loop로 모두 처리.
   */
  console.log("event:", JSON.stringify(event));

  const records = event.Records ?? [];

  for (const record of records) {
    /**
     * 1) 이벤트에서 bucket, key 뽑기
     * - key는 URL 인코딩되어 들어올 수 있고 + 가 공백을 의미할 때가 있어 디코딩 처리한다.
     */
    const bucket = record.s3.bucket.name;
    const originKey = decodeURIComponent(
      record.s3.object.key.replace(/\+/g, " ")
    );

    /**
     * 2) 처리 대상인지 필터링
     *
     * 처리 대상:
     *  - profile/origin/{id}/{uuid}.png
     *  - company/origin/{id}/{uuid}.png
     *  - serviceItem/origin/{id}/{uuid}.png
     *
     * 제외 대상(루프 방지):
     *  - 이미 생성된 thumb/resize 경로는 다시 처리하면 무한 루프 가능
     */
    const isOrigin =
      originKey.startsWith("profile/origin/") ||
      originKey.startsWith("company/origin/") ||
      originKey.startsWith("serviceItem/origin/");

    const isGeneratedVariant =
      originKey.includes("/thumb/") || originKey.includes("/resize/");

    if (!isOrigin || isGeneratedVariant) {
      console.log("skip key:", originKey);
      continue;
    }

    /**
     * 3) key 파싱 (규칙 기반)
     * 기대하는 포맷: {root}/{variant}/{ownerId}/{filename}
     * 예) profile/origin/123/abcd.png
     *
     * parts:
     *  - root   : profile | company | serviceItem
     *  - variant: origin
     *  - ownerId: userId/companyId/serviceItemOwnerId (프로젝트 정의에 따라)
     *  - filename: uuid.ext
     */
    const parts = originKey.split("/");
    if (parts.length !== 4) {
      console.warn("invalid key format:", originKey);
      continue;
    }

    const root = parts[0]; // profile | company | serviceItem
    const variant = parts[1]; // origin (사실상 고정)
    const ownerId = parts[2]; // 주인 ID
    const filename = parts[3]; // uuid.ext

    // variant가 origin이 아닌 예외 케이스면 방어적으로 skip
    if (variant !== "origin") {
      console.warn("not origin variant:", originKey);
      continue;
    }

    /**
     * 4) 파일 확장자 파싱 및 sharp output format 결정
     *
     * - sharp는 jpg 확장자를 jpeg 포맷으로 취급
     * - 우리가 허용할 이미지 포맷: jpeg/png/webp
     * - 그 외 확장자는 안전하게 jpeg로 변환
     */
    const nameParts = filename.split(".");
    if (nameParts.length < 2) {
      console.warn("no extension:", filename);
      continue;
    }

    const uuid = nameParts.slice(0, -1).join(".");
    const extRaw = nameParts[nameParts.length - 1].toLowerCase();

    const format = extRaw === "jpg" ? "jpeg" : extRaw;
    const allowed = new Set(["jpeg", "png", "webp"]);
    const outFormat = allowed.has(format) ? format : "jpeg";
    const outExt = outFormat === "jpeg" ? "jpg" : outFormat;

    /**
     * 5) root별 정책 결정
     * - serviceItem: 256x256, 결과 경로는 resize
     * - profile/company: 128x128, 결과 경로는 thumb
     */
    const isServiceItem = root === "serviceItem";

    const size = isServiceItem ? 256 : 128;
    const targetKey = isServiceItem
      ? `${root}/resize/${ownerId}/${uuid}.${outExt}`
      : `${root}/thumb/${ownerId}/${uuid}.${outExt}`;

    /**
     * 6) 멱등성 보장
     * - 이미 targetKey가 존재하면(이전 처리 성공) 다시 작업하지 않는다.
     * - S3는 overwrite도 가능하지만, 중복 처리/비용 증가/경쟁 상태 방지를 위해 "존재하면 skip" 정책이 안전.
     */
    try {
      await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: targetKey }));
      console.log("already exists. skip:", targetKey);
      continue;
    } catch (e) {
      // 존재하지 않으면 HeadObject가 에러 -> 정상 흐름(continue 하지 않고 진행)
    }

    /**
     * 7) 원본 이미지 다운로드(GetObject)
     * - S3 GetObject 응답 Body는 stream이므로 buffer로 변환해야 sharp가 처리 가능.
     */
    const originObj = await s3.send(
      new GetObjectCommand({ Bucket: bucket, Key: originKey })
    );
    const originBuffer = await streamToBuffer(originObj.Body);

    /**
     * 8) 리사이즈/썸네일 생성(sharp)
     * - fit: "cover"로 지정하면 center crop으로 꽉 채우게 된다.
     * - position: "centre"는 중앙 기준 crop
     */
    const outBuffer = await sharp(originBuffer)
      .resize(size, size, { fit: "cover", position: "centre" })
      .toFormat(outFormat)
      .toBuffer();

    /**
     * 9) Content-Type 설정
     * - CDN/브라우저 캐싱, 올바른 렌더링을 위해 지정
     */
    const contentType =
      outFormat === "jpeg"
        ? "image/jpeg"
        : outFormat === "png"
        ? "image/png"
        : "image/webp";

    /**
     * 10) 결과 업로드(PutObject)
     * - ✅ 삭제하지 않고 복사 생성만 한다 (origin은 유지)
     * - CacheControl로 장기 캐싱 허용 (이미지 url/key가 immutable하다는 전제: uuid 기반)
     */
    await s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: targetKey,
        Body: outBuffer,
        ContentType: contentType,
        CacheControl: "public, max-age=31536000, immutable",
      })
    );

    /**
     * 11) SQS 완료 메시지 발행
     * - Spring에서 이 메시지를 받아 DB 메타데이터 갱신(예: thumbKey 저장, isActive 전환 등)
     * - serviceItem의 경우 targetKey가 resizeKey가 됨
     *
     * NOTE: 메시지 스키마는 Spring DTO와 맞춰야 한다.
     *       (이 예시에서는 resultKey로 통일)
     */
    if (QUEUE_URL) {
      await sqs.send(
        new SendMessageCommand({
          QueueUrl: QUEUE_URL,
          MessageBody: JSON.stringify({
            status: "DONE",
            bucket,
            originKey,
            resultKey: targetKey, // thumbKey 또는 resizeKey를 통일해서 전달
            root,
            ownerId,
            ts: new Date().toISOString(),
          }),
        })
      );
    } else {
      console.warn("THUMBNAIL_QUEUE_URL not set; skip sqs send");
    }

    console.log("generated:", targetKey);
  }

  /**
   * Lambda 핸들러는 정상 종료 시 응답을 반환하면 된다.
   */
  return { status: "ok" };
};

/**
 * S3 GetObject의 Body(stream)을 Buffer로 변환하는 유틸
 * - Node.js 런타임에서 stream을 그대로 sharp에 넣을 수 없으므로 buffer 변환 필요
 */
async function streamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}
