/**
 * 제로지스톡 (zerozi-stock)
 * kiwoomapi D1(naver_sise)에서 실시간포착 편입 종목을 읽어
 * Cloudflare Workers AI(Llama)로 시황 글 생성 → Blogger 자동 포스팅
 *
 * - kiwoomapi worker.js와 완전 분리, D1은 읽기전용 조회만 사용
 * - cron: 0 0-6 * * 1-5 (UTC) = KST 09:00~15:00, 매시 정각
 */

const BLOGGER_API = 'https://www.googleapis.com/blogger/v3/blogs';

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

const MAX_STOCKS_PER_POST = 3; // 상세 설명이 길어서 한 번에 너무 많으면 응답이 잘림

async function runPostingJob(env) {
  const stocks = await getFreshWatchlistStocks(env);
  if (stocks.length === 0) {
    return { ok: true, posted: false, reason: 'no fresh stocks' };
  }

  let alreadyPosted = await filterAlreadyPosted(env, stocks);
  if (alreadyPosted.length === 0) {
    return { ok: true, posted: false, reason: 'all already posted today' };
  }
  // 초과분은 다음 실행 회차에서 자동으로 이어서 포스팅됨 (posted 처리 안 하므로 다음 /run에서 다시 픽업)
  alreadyPosted = alreadyPosted.slice(0, MAX_STOCKS_PER_POST);

  // 종목별 뉴스+차트를 병렬로 동시 조회 (순차 처리 대비 대폭 단축)
  await Promise.all(alreadyPosted.map(async s => {
    [s.news, s.chartUrl] = await Promise.all([
      fetchStockNews(s.name),
      buildIntradayChartUrl(env, s.code, s.name)
    ]);
  }));

  const { title, content } = await generateArticle(env, alreadyPosted);
  const accessToken = await getAccessToken(env);
  const post = await postToBlogger(env, accessToken, title, content);

  await markPosted(env, alreadyPosted);

  return { ok: true, posted: true, postUrl: post.url, stocks: alreadyPosted.map(s => s.code) };
}

// 09:00(KST)~현재까지 snapshots의 실제 가격 데이터로 QuickChart 정적 이미지 URL 생성
async function buildIntradayChartUrl(env, code, name) {
  try {
    const todayUtc = new Date().toISOString().slice(0, 10); // KST 09:00 = UTC 00:00 같은 날짜
    const since = `${todayUtc}T00:00:00.000Z`;
    const { results } = await env.DB.prepare(
      `SELECT price, captured_at FROM snapshots
       WHERE code = ? AND captured_at >= ?
       ORDER BY captured_at ASC LIMIT 500`
    ).bind(code, since).all();

    if (!results || results.length < 2) return null;

    const labels = results.map(r =>
      new Date(r.captured_at).toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit', hour12: false, timeZone: 'Asia/Seoul' })
    );
    const prices = results.map(r => r.price);

    const chartConfig = {
      type: 'line',
      data: {
        labels,
        datasets: [{
          label: `${name} (09:00~현재, 1분봉)`,
          data: prices,
          borderColor: '#0B1E3D',
          backgroundColor: 'rgba(11,30,61,0.08)',
          fill: true,
          pointRadius: 0,
          borderWidth: 2
        }]
      },
      options: {
        plugins: { legend: { display: true, labels: { font: { size: 11 } } } },
        scales: {
          x: { ticks: { maxTicksLimit: 8, font: { size: 10 } } },
          y: { ticks: { font: { size: 10 } } }
        }
      }
    };

    const res = await fetch('https://quickchart.io/chart/create', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ chart: chartConfig, backgroundColor: 'white', width: 640, height: 300, version: '4' })
    });
    if (!res.ok) return null;
    const data = await res.json();
    return data.success ? data.url : null;
  } catch {
    return null; // 차트 생성 실패해도 포스팅 자체는 계속 진행
  }
}

// Google 뉴스 RSS에서 종목명 관련 최신 기사 1건(제목+실제 링크) 조회
async function fetchStockNews(stockName) {
  try {
    const q = encodeURIComponent(`${stockName} 주식`);
    const res = await fetch(`https://news.google.com/rss/search?q=${q}&hl=ko&gl=KR&ceid=KR:ko`);
    if (!res.ok) return null;
    const xml = await res.text();
    const itemMatch = xml.match(/<item>([\s\S]*?)<\/item>/);
    if (!itemMatch) return null;
    const item = itemMatch[1];
    const titleMatch = item.match(/<title>([\s\S]*?)<\/title>/);
    const linkMatch = item.match(/<link>([\s\S]*?)<\/link>/);
    if (!titleMatch || !linkMatch) return null;
    const title = titleMatch[1].replace(/<!\[CDATA\[|\]\]>/g, '').trim();
    const link = linkMatch[1].trim();
    return { title, link };
  } catch {
    return null; // 뉴스 조회 실패해도 포스팅 자체는 계속 진행
  }
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
    `- ${s.name}(${s.code}): 신호=${s.added_state || '-'}, 진입가=${s.entry_price ?? '-'}, 등락률=${s.change_rate != null ? s.change_rate + '%' : '정보없음'}${s.news ? `, 관련뉴스="${s.news.title}"` : ''}`
  ).join('\n');

  const prompt = `너는 15년 경력의 한국 주식시장 애널리스트야. 아래는 실시간 조건검색으로 방금 포착된 급등 신호 종목 리스트야.
증권사 데일리 노트처럼 짧고 명쾌하게 작성해줘. 미사여구, 장황한 배경 설명 없이 핵심만.

[포착 종목]
${listText}

요구사항:
- 제목: 간결하고 핵심이 드러나는 한 줄 (종목명 포함)
- 도입부: 1~2문장으로 오늘 포착 종목군의 공통 특징만 짧게
- 종목별 섹션(각 종목마다 <h3> 소제목으로 "종목명(코드)" 구분):
  각 섹션은 2~3문장으로 짧게, 다음을 압축해서 담을 것:
  - 포착 신호(added_state)와 현재 가격 흐름(등락률) 한 줄 요약
  - 관련뉴스가 있으면 제목을 자연스럽게 한 문장에 녹여서 언급 (링크는 절대 만들지 마, 별도로 삽입됨)
  - 체크포인트 한 가지만 간결하게
  - 불필요한 수식어, 반복 설명 금지. 애널리스트가 시간 없을 때 쓰는 메모 톤
- 마무리: 1문장 총평
- 마지막 줄에 "본 글은 투자 참고용이며 투자 판단의 책임은 본인에게 있습니다" 문구 포함
- HTML 태그 사용 (h3, p, strong, ul, li) — 위 요구사항 그대로 h3로 종목 구분할 것
- 절대 <a> 태그나 URL을 직접 작성하지 마 (링크는 별도 시스템이 삽입함)
- 출력 형식 (다른 설명 없이 정확히 이 형식만):
제목: (여기에 제목 한 줄)
===본문시작===
(여기에 HTML 본문)`;

  const aiResponse = await env.AI.run('@cf/meta/llama-3.1-8b-instruct-fast', {
    messages: [{ role: 'user', content: prompt }],
    max_tokens: 2000
  });

  const rawText = aiResponse.response || '';
  const marker = '===본문시작===';
  const markerIdx = rawText.indexOf(marker);
  if (markerIdx === -1) {
    throw new Error(`Workers AI 출력 형식 오류 (구분자 없음) / raw: ${rawText.slice(0, 300)}`);
  }
  const titleLine = rawText.slice(0, markerIdx).trim();
  const title = titleLine.replace(/^제목\s*[:：]\s*/, '').trim();
  const rawContent = rawText.slice(markerIdx + marker.length).trim().replace(/^```html\s*|```$/g, '').trim();
  if (!title || !rawContent) {
    throw new Error(`Workers AI 출력 파싱 실패 (제목/본문 비어있음) / raw: ${rawText.slice(0, 300)}`);
  }
  const parsed = { title, content: rawContent };

  // 종목명(코드) h3 소제목 뒤에 로고(실패 시 컬러 배지 폴백) 삽입
  let content = injectLogoBadges(parsed.content, stocks);
  // h3 섹션 바로 다음에 실제 뉴스 링크 삽입 (Gemini가 만든 링크 아님, 신뢰 가능한 실제 URL)
  content = injectNewsLinks(content, stocks);
  // 종목별 실시간(장중 5분봉) 차트 위젯 삽입
  content = injectCharts(content, stocks);

  return { title: parsed.title, content };
}

// 09:00~현재 실제 가격으로 그린 정적 차트 이미지를 종목 섹션 하단에 삽입 (없으면 스킵)
function injectCharts(html, stocks) {
  let result = html;
  for (const s of stocks) {
    if (!s.chartUrl) continue;
    const chartHtml = `<div style="margin:0 0 20px;border:1px solid #E4E7EC;border-radius:8px;overflow:hidden;padding:8px;background:#fff;"><img src="${s.chartUrl}" alt="${escapeHtml(s.name)} 09시~현재 1분봉 차트" style="width:100%;height:auto;display:block;"></div>`;
    const afterNewsPattern = new RegExp(`(<h3[^>]*>[\\s\\S]*?${escapeRegex(s.name)}[\\s\\S]*?\\(${escapeRegex(s.code)}\\)[\\s\\S]*?</h3>(?:\\s*<p[^>]*>📰[\\s\\S]*?</p>)?)`);
    result = result.replace(afterNewsPattern, `$1${chartHtml}`);
  }
  return result;
}

// h3 종목 섹션 바로 뒤에 실제 뉴스 링크(제목 그대로, 실제 URL)를 삽입
function injectNewsLinks(html, stocks) {
  let result = html;
  for (const s of stocks) {
    if (!s.news) continue;
    const newsHtml = `<p style="font-size:13px;color:#6B7280;margin:-4px 0 14px;">📰 관련뉴스: <a href="${s.news.link}" target="_blank" rel="noopener">${escapeHtml(s.news.title)}</a></p>`;
    const pattern = new RegExp(`(</h3>)`, 'g');
    // 종목별로 정확히 해당 h3만 타겟팅하기 위해 종목명 포함 h3 뒤에만 삽입
    const h3Pattern = new RegExp(`(<h3[^>]*>[\\s\\S]*?${escapeRegex(s.name)}[\\s\\S]*?\\(${escapeRegex(s.code)}\\)[\\s\\S]*?</h3>)`);
    result = result.replace(h3Pattern, `$1${newsHtml}`);
  }
  return result;
}

function escapeHtml(str) {
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// 종목명(코드) h3 소제목 앞에 컬러 원형 배지(로고 대용) 삽입 - 외부 이미지 의존 없이 항상 정상 표시
function injectLogoBadges(html, stocks) {
  let result = html;
  for (const s of stocks) {
    const initial = (s.name || '').slice(0, 1);
    const color = hashToColor(s.name || s.code);
    const badge = `<span style="display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;border-radius:50%;background:${color};color:#fff;font-size:13px;font-weight:800;margin-right:8px;vertical-align:middle;">${initial}</span>`;
    const pattern = new RegExp(`(<h3[^>]*>)(\\s*${escapeRegex(s.name)}\\s*\\(${escapeRegex(s.code)}\\))`, 'g');
    result = result.replace(pattern, `$1${badge}$2`);
  }
  return result;
}

function escapeRegex(str) {
  return String(str).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function hashToColor(str) {
  const palette = ['#0B1E3D', '#C9A24B', '#2255C4', '#D93B3B', '#146C5C', '#7A4FA3', '#B15C1E'];
  let hash = 0;
  for (let i = 0; i < str.length; i++) hash = str.charCodeAt(i) + ((hash << 5) - hash);
  return palette[Math.abs(hash) % palette.length];
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
