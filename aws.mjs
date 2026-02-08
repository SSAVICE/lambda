/**
 * AWS S3 / SQS 헬퍼 모듈
 *
 * 모든 외부 호출에 지수 백오프 재시도(withRetry)를 적용한다.
 * - 최대 3회 재시도 (총 4회 시도)
 * - 대기: 200ms → 400ms → 800ms + jitter
 */
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";

const s3 = new S3Client({});
const sqs = new SQSClient({});

/** 처리 완료를 Spring 백엔드에 알릴 SQS 큐 URL (Lambda 환경변수) */
const QUEUE_URL = process.env.THUMBNAIL_QUEUE_URL;
const MAX_RETRIES = 3;
const BASE_DELAY_MS = 200;

// ─── retry ───────────────────────────────────────────────────

/**
 * fn을 최대 MAX_RETRIES회 재시도한다.
 * 실패할 때마다 지수 백오프 + 랜덤 jitter(0~100ms)로 대기.
 * @param {Function} fn    - 재시도할 async 함수
 * @param {string}   label - 로그에 표시할 작업 이름
 */
async function withRetry(fn, label) {
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (attempt === MAX_RETRIES) {
        console.error(`${label} failed after ${MAX_RETRIES + 1} attempts:`, err);
        throw err;
      }
      const delay = BASE_DELAY_MS * 2 ** attempt + Math.random() * 100;
      console.warn(`${label} attempt ${attempt + 1} failed, retrying in ${Math.round(delay)}ms...`);
      await new Promise((r) => setTimeout(r, delay));
    }
  }
}

// ─── S3 helpers ──────────────────────────────────────────────

/** HeadObject로 S3 키 존재 여부 확인 (멱등성 체크용) */
export async function objectExists(bucket, key) {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    return true;
  } catch {
    return false;
  }
}

/** 원본 이미지를 S3에서 다운로드하여 Buffer로 반환 */
export async function downloadOrigin(bucket, key) {
  const res = await withRetry(
    () => s3.send(new GetObjectCommand({ Bucket: bucket, Key: key })),
    `GetObject(${key})`,
  );
  return streamToBuffer(res.Body);
}

/** 썸네일 Buffer를 S3에 업로드 (immutable 캐시 헤더 포함) */
export async function uploadThumb(bucket, key, body, contentType) {
  await withRetry(
    () =>
      s3.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: body,
          ContentType: contentType,
          CacheControl: "public, max-age=31536000, immutable",
        }),
      ),
    `PutObject(${key})`,
  );
}

// ─── SQS helper ──────────────────────────────────────────────

/** 처리 완료 메시지를 SQS로 전송. QUEUE_URL 미설정 시 경고만 출력 */
export async function notifyComplete(message) {
  if (!QUEUE_URL) {
    console.warn("THUMBNAIL_QUEUE_URL not set; skip SQS send");
    return;
  }
  await withRetry(
    () =>
      sqs.send(
        new SendMessageCommand({
          QueueUrl: QUEUE_URL,
          MessageBody: JSON.stringify(message),
        }),
      ),
    `SQS(${message.root}/${message.ownerId})`,
  );
}

// ─── util ────────────────────────────────────────────────────

/** S3 GetObject 응답의 Body(stream)를 Buffer로 변환 */
async function streamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}
