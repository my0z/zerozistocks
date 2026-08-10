/**
 * 제로지스톡 (zerozi-stock)
 * kiwoomapi D1(naver_sise)에서 실시간포착 편입 종목을 읽어
 * Gemini로 시황 글 생성 → Blogger 자동 포스팅
 *
 * - kiwoomapi worker.js와 완전 분리, D1은 읽기전용 조회만 사용
 * - cron: 0 0-6 * * 1-5 (UTC) = KST 09:00~15:00, 매시 정각
 */

const BLOGGER_API = 'https://www.googleapis.com/blogger/v3/blogs';
const GEMINI_API = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent';

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runPostingJob(env));
  },

  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === '/run') {
      const result = await runPostingJob(env);
      return new Response(JSON.stringify(result, null, 2), {
        headers: { 'content-type': 'application/json; charset=utf-8' }
      });
    }
    return new Response('zerozi-stock worker', { status: 200 });
  }
};

async function runPostingJob(env) {
  const stocks = await getFreshWatchlistStocks(env);
  if (stocks.length === 0) {
    return { ok: true, posted: false, reason: 'no fresh stocks' };
  }

  const alreadyPosted = await filterAlreadyPosted(env, stocks);
  if (alreadyPosted.length === 0) {
    return { ok: true, posted: false, reason: 'all already posted today' };
  }

  const { title, content } = await generateArticle(env, alreadyPosted);
  const accessToken = await getAccessToken(env);
  const post = await postToBlogger(env, accessToken, title, content);

  await markPosted(env, alreadyPosted);

  return { ok: true, posted: true, postUrl: post.url, stocks: alreadyPosted.map(s => s.code) };
}

// D1에서 최근 1시간 이내 실시간포착 편입 종목 조회 (읽기전용)
async function getFreshWatchlistStocks(env) {
  const since = new Date(Date.now() - 60 * 60 * 1000)
    .toISOString()
    .slice(0, 19)
    .replace('T', ' '); // D1 datetime('now') 형식(YYYY-MM-DD HH:MM:SS)에 맞춤
  const { results } = await env.DB.prepare(
    `SELECT
       w.code, w.name, w.added_state, w.added_at, w.entry_price,
       (SELECT s.change_rate FROM snapshots s
        WHERE s.code = w.code
        ORDER BY s.captured_at DESC LIMIT 1) AS change_rate
     FROM watchlist w
     WHERE w.source_board = '실시간포착'
       AND w.added_at >= ?
     ORDER BY w.added_at DESC
     LIMIT 20`
  ).bind(since).all();
  return results || [];
}

// 당일 중복 포스팅 방지 (KV 사용)
async function filterAlreadyPosted(env, stocks) {
  const today = new Date().toISOString().slice(0, 10);
  const fresh = [];
  for (const s of stocks) {
    const key = `posted:${today}:${s.code}`;
    const exists = await env.ZEROZI_KV.get(key);
    if (!exists) fresh.push(s);
  }
  return fresh;
}

async function markPosted(env, stocks) {
  const today = new Date().toISOString().slice(0, 10);
  for (const s of stocks) {
    const key = `posted:${today}:${s.code}`;
    await env.ZEROZI_KV.put(key, '1', { expirationTtl: 60 * 60 * 24 * 2 });
  }
}

async function generateArticle(env, stocks) {
  const listText = stocks.map(s =>
    `- ${s.name}(${s.code}): 신호=${s.added_state || '-'}, 진입가=${s.entry_price ?? '-'}, 등락률=${s.change_rate != null ? s.change_rate + '%' : '정보없음'}`
  ).join('\n');

  const prompt = `너는 한국 주식 시황 블로거야. 아래는 실시간 조건검색으로 방금 포착된 급등 신호 종목 리스트야.
이 데이터를 근거로 블로그 포스팅용 시황 글을 작성해줘.

[포착 종목]
${listText}

요구사항:
- 제목: 흥미를 끄는 한 줄 (종목명 1~2개 포함 가능)
- 본문: 각 종목의 포착 배경과 신호 의미를 자연스럽게 설명 (투자 권유 아님, 정보 제공 목적 명시)
- 마지막에 "본 글은 투자 참고용이며 투자 판단의 책임은 본인에게 있습니다" 문구 포함
- HTML 태그 사용 가능 (p, strong, ul, li)
- 출력 형식은 반드시 아래 JSON만: {"title": "...", "content": "..."}`;

  const res = await fetch(`${GEMINI_API}?key=${env.GEMINI_API_KEY}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      contents: [{ parts: [{ text: prompt }] }],
      generationConfig: { responseMimeType: 'application/json' }
    })
  });

  if (!res.ok) {
    throw new Error(`Gemini API error: ${res.status} ${await res.text()}`);
  }

  const data = await res.json();
  const text = data.candidates?.[0]?.content?.parts?.[0]?.text || '{}';
  const parsed = JSON.parse(text.replace(/```json|```/g, '').trim());
  return { title: parsed.title, content: parsed.content };
}

// 기존 geminai-worker.js와 동일한 refresh_token 플로우 재사용
async function getAccessToken(env) {
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: env.GOOGLE_CLIENT_ID,
      client_secret: env.GOOGLE_CLIENT_SECRET,
      refresh_token: env.GOOGLE_REFRESH_TOKEN,
      grant_type: 'refresh_token'
    })
  });
  if (!res.ok) {
    throw new Error(`OAuth token error: ${res.status} ${await res.text()}`);
  }
  const data = await res.json();
  return data.access_token;
}

async function postToBlogger(env, accessToken, title, content) {
  const res = await fetch(`${BLOGGER_API}/${env.ZEROZI_BLOG_ID}/posts/`, {
    method: 'POST',
    headers: {
      'authorization': `Bearer ${accessToken}`,
      'content-type': 'application/json'
    },
    body: JSON.stringify({ title, content })
  });
  if (!res.ok) {
    throw new Error(`Blogger API error: ${res.status} ${await res.text()}`);
  }
  return res.json();
}
