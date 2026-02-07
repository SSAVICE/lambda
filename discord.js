/**
 * Discord Webhook 알림 모듈
 *
 * Lambda 처리 실패 시 Discord 채널로 에러 알림을 전송한다.
 * 환경변수 DISCORD_WEBHOOK_URL이 설정되어 있어야 동작한다.
 */

const WEBHOOK_URL = process.env.DISCORD_WEBHOOK_URL;

/**
 * 처리 실패 정보를 Discord embed 메시지로 전송한다.
 *
 * @param {string} originKey - 실패한 S3 object key
 * @param {Error}  error     - 발생한 에러
 */
export async function notifyFailure(originKey, error) {
  if (!WEBHOOK_URL) {
    console.warn("DISCORD_WEBHOOK_URL not set; skip discord notify");
    return;
  }

  const payload = {
    embeds: [
      {
        title: "Lambda 썸네일 처리 실패",
        color: 0xff0000,
        fields: [
          { name: "S3 Key", value: `\`${originKey}\`` },
          { name: "Error", value: `\`\`\`${error.message}\`\`\`` },
        ],
        timestamp: new Date().toISOString(),
      },
    ],
  };

  try {
    const res = await fetch(WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      console.error(`Discord webhook failed: ${res.status}`);
    }
  } catch (err) {
    // Discord 알림 실패가 Lambda 전체를 죽이면 안 됨
    console.error("Discord notify error:", err);
  }
}
