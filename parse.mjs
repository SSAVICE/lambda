/**
 * S3 키 파싱 및 이미지 포맷 결정 모듈
 *
 * S3 키 규칙:
 *   {root}/origin/{ownerId}/{filename}   → 원본
 *   {root}/thumb/{ownerId}/{filename}    → 썸네일
 *
 * root: "profile" | "company" | "serviceItem"
 */

const ALLOWED_FORMATS = new Set(["jpeg", "png", "webp"]);

/**
 * S3 origin 키를 파싱한다.
 *
 * 유효 조건:
 *  - {root}/origin/{ownerId}/{filename} 형식일 것
 *  - root가 profile / company / serviceItem 중 하나일 것
 *  - thumb/resize 경로가 아닐 것 (루프 방지)
 *  - serviceItem은 thumb_ prefix 파일만 대상
 *
 * @param  {string} originKey - S3 object key
 * @return {{ root: string, ownerId: string, filename: string } | null}
 */
export function parseOriginKey(originKey) {
  const isOrigin =
    originKey.startsWith("profile/origin/") ||
    originKey.startsWith("company/origin/") ||
    originKey.startsWith("serviceItem/origin/");

  // 루프 방지: Lambda가 생성한 thumb/resize 경로는 제외
  if (!isOrigin || originKey.includes("/thumb/")) {
    return null;
  }

  // {root}/origin/{ownerId}/{filename}
  const parts = originKey.split("/");
  if (parts.length !== 4 || parts[1] !== "origin") return null;

  const [root, , ownerId, filename] = parts;

  // serviceItem은 thumb_ prefix 파일만 썸네일 생성 대상
  if (root === "serviceItem" && !filename.startsWith("thumb_")) return null;

  return { root, ownerId, filename };
}

/**
 * 파일명에서 출력 포맷 정보를 결정한다.
 *
 * - jpg → jpeg로 정규화
 * - 허용 포맷(jpeg/png/webp) 외에는 jpeg로 폴백
 *
 * @param  {string} filename - 예: "abcd-1234.png"
 * @return {{ uuid: string, outFormat: string, outExt: string, contentType: string } | null}
 */
export function resolveFormat(filename) {
  const dotIdx = filename.lastIndexOf(".");
  if (dotIdx <= 0) return null;

  const uuid = filename.slice(0, dotIdx);
  const extRaw = filename.slice(dotIdx + 1).toLowerCase();
  const format = extRaw === "jpg" ? "jpeg" : extRaw;
  const outFormat = ALLOWED_FORMATS.has(format) ? format : "jpeg";
  const outExt = outFormat === "jpeg" ? "jpg" : outFormat;
  const contentType = `image/${outFormat}`;

  return { uuid, outFormat, outExt, contentType };
}
