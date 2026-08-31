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
    if (event.cron === '1 0-6 * * 1-5') {
      ctx.waitUntil(runExitReportJob(env, 'take'));
    } else if (event.cron === '2 0-6 * * 1-5') {
      ctx.waitUntil(runExitReportJob(env, 'stop'));
    } else {
      ctx.waitUntil(runPostingJob(env));
    }
  },

  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === '/run') {
      const result = await runPostingJob(env);
      return new Response(JSON.stringify(result, null, 2), {
        headers: { 'content-type': 'application/json; charset=utf-8' }
      });
    }
    if (url.pathname === '/run-profits') {
      const result = await runExitReportJob(env, 'take');
      return new Response(JSON.stringify(result, null, 2), {
        headers: { 'content-type': 'application/json; charset=utf-8' }
      });
    }
    if (url.pathname === '/run-stoploss') {
      const result = await runExitReportJob(env, 'stop');
      return new Response(JSON.stringify(result, null, 2), {
        headers: { 'content-type': 'application/json; charset=utf-8' }
      });
    }
    return new Response('zerozi-stock worker', { status: 200 });
  }
};

const MAX_STOCKS_PER_POST = 6; // 상세 설명이 길어서 한 번에 너무 많으면 응답이 잘림

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
    const [news, riskLevels] = await Promise.all([
      fetchStockNews(s.name),
      getRiskLevels(env, s.code)
    ]);
    s.news = news;
    s.chartUrl = await buildIntradayChartUrl(env, s.code, s.name, null, riskLevels);
  }));

  const { title, content } = await generateArticle(env, alreadyPosted);
  const accessToken = await getAccessToken(env);
  const post = await postToBlogger(env, accessToken, title, content);

  await markPosted(env, alreadyPosted);

  return { ok: true, posted: true, postUrl: post.url, stocks: alreadyPosted.map(s => s.code) };
}

// 최근 1시간 내 익절/손절 처리된 종목을 모아 발행 (매시 정각+1분, +2분)
// kind: 'take' (익절삭제) | 'stop' (손절삭제)
async function runExitReportJob(env, kind) {
  const stocks = await getRecentExitStocks(env, kind);
  if (stocks.length === 0) {
    return { ok: true, posted: false, reason: `no recent ${kind}-exit stocks` };
  }

  const alreadyPosted = await filterAlreadyPostedExit(env, stocks, kind);
  if (alreadyPosted.length === 0) {
    return { ok: true, posted: false, reason: 'all already posted this hour' };
  }

  // 이탈 이후 현재까지 등락 추적: 최신 시세를 snapshots에서 조회
  const exitLabel = kind === 'take' ? '익절' : '손절';
  await Promise.all(alreadyPosted.map(async s => {
    s.currentPrice = await getLatestPrice(env, s.code);
    s.chartUrl = await buildIntradayChartUrl(env, s.code, s.name, {
      at: s.recorded_at,
      price: s.later_price,
      label: exitLabel
    });
  }));

  const { title, content } = await generateExitArticle(env, alreadyPosted, kind);
  const accessToken = await getAccessToken(env);
  const post = await postToBlogger(env, accessToken, title, content);

  await markPostedExit(env, alreadyPosted, kind);

  return { ok: true, posted: true, postUrl: post.url, stocks: alreadyPosted.map(s => s.code) };
}

// watchlist_performance에서 최근 1시간 내 익절/손절 표시된 종목 조회 (읽기전용)
async function getRecentExitStocks(env, kind) {
  const label = kind === 'take' ? '익절삭제' : '손절삭제';
  const { results } = await env.DB.prepare(
    `SELECT code, name, entry_price, later_price, pnl_pct, recorded_at
     FROM watchlist_performance
     WHERE added_state LIKE ?
       AND recorded_at >= datetime('now', '-1 hour') || '.000Z'
     ORDER BY recorded_at DESC
     LIMIT 40`
  ).bind(`%${label}%`).all();
  if (!results) return [];
  // 같은 종목이 horizon_min(30/60분) 등으로 여러 줄 기록될 수 있어 code 기준 최신 1건만 유지
  const seen = new Set();
  const deduped = [];
  for (const r of results) {
    if (seen.has(r.code)) continue;
    seen.add(r.code);
    deduped.push(r);
  }
  return deduped.slice(0, 20);
}

// 종목코드+시각 조합 기준 당일 중복 발행 방지 (KV 사용, 익절/손절 리포트 각각 별도 키 공간)
async function filterAlreadyPostedExit(env, stocks, kind) {
  const today = new Date().toISOString().slice(0, 10);
  const fresh = [];
  for (const s of stocks) {
    const key = `posted-${kind}:${today}:${s.code}:${s.recorded_at}`;
    const exists = await env.ZEROZI_KV.get(key);
    if (!exists) fresh.push(s);
  }
  return fresh;
}

async function markPostedExit(env, stocks, kind) {
  const today = new Date().toISOString().slice(0, 10);
  for (const s of stocks) {
    const key = `posted-${kind}:${today}:${s.code}:${s.recorded_at}`;
    await env.ZEROZI_KV.put(key, '1', { expirationTtl: 60 * 60 * 24 * 2 });
  }
}

// snapshots에서 해당 종목의 가장 최근 가격 조회 (익절/손절 후 등락 계산용)
async function getLatestPrice(env, code) {
  try {
    const row = await env.DB.prepare(
      `SELECT price FROM snapshots WHERE code = ? ORDER BY captured_at DESC LIMIT 1`
    ).bind(code).first();
    return row ? row.price : null;
  } catch {
    return null;
  }
}

// 익절/손절 리포트 공용 Llama 프롬프트 - 이탈가/현재가 기준 이후 흐름을 중심으로 짧게 작성
async function generateExitArticle(env, stocks, kind) {
  const label = kind === 'take' ? '익절' : '손절';
  const listText = stocks.map(s => {
    const afterPct = (s.currentPrice != null && s.later_price)
      ? (((s.currentPrice - s.later_price) / s.later_price) * 100).toFixed(2)
      : null;
    return `- ${s.name}(${s.code}): 진입가=${s.entry_price}, ${label}시점가=${s.later_price}, ${label}시 수익률=${s.pnl_pct}%` +
      (afterPct != null ? `, ${label} 이후 현재가=${s.currentPrice}(${label}가 대비 ${afterPct}%)` : '');
  }).join('\n');

  const situationHint = kind === 'take'
    ? '목표가 도달로 워치리스트에서 정리된'
    : '손절 기준에 걸려 워치리스트에서 정리된';
  const evalHint = kind === 'take'
    ? '"더 갈 수 있었다/제때 나왔다" 류 짧은 평가'
    : '"손절이 적절했다/이후 반등했다" 류 짧은 평가';

  const prompt = `너는 15년 경력의 한국 주식시장 애널리스트야. 아래는 최근 1시간 내 ${situationHint} 종목 리스트야.
증권사 데일리 노트처럼 짧고 명쾌하게 "사후 검증" 리포트를 작성해줘.

[${label} 종목]
${listText}

요구사항:
- 제목: 간결한 한 줄 (예: "오늘 ${label} 종목 점검" 류, 종목명 포함 가능)
- 도입부: 1~2문장으로 이번 시간 ${label} 종목군의 공통 특징만 짧게
- 종목별 섹션(각 종목마다 <h3> 소제목으로 "종목명(코드)" 구분), 각각 2~3문장:
  1문단) 진입가 대비 ${label} 시점 수익률 요약
  2문단) ${label} 이후 현재까지 추가로 올랐는지 내렸는지 (데이터 있으면 수치로, ${evalHint})
  각 문단 1문장, 불필요한 수식어 금지
- 마무리: 1문장 총평
- 마지막 줄에 "본 글은 투자 참고용이며 투자 판단의 책임은 본인에게 있습니다" 문구 포함
- HTML 태그 사용 (h3, p, strong, ul, li)
- 출력 형식 (다른 설명 없이 정확히 이 형식만):
제목: (여기에 제목 한 줄)
===본문시작===
(여기에 HTML 본문)`;

  const aiResponse = await env.AI.run('@cf/meta/llama-3.1-8b-instruct-fast', {
    messages: [{ role: 'user', content: prompt }],
    max_tokens: 3500
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

  let content = injectLogoBadges(rawContent, stocks);
  content = injectCharts(content, stocks);
  content = `<p style="font-size:12px;color:#9AA0AC;margin-bottom:14px;">⏱ 발행: ${formatKstTimestamp()}</p>` + content;

  return { title, content };
}

// 09:00(KST)~현재까지 snapshots의 실제 가격 데이터로 SVG 차트를 직접 그려 data URI 반환
// (외부 차트 서비스 의존 없음 - 네트워크 요청 자체가 없어 빠르고 항상 안정적으로 렌더링됨)
async function buildIntradayChartUrl(env, code, name, exitPoint = null, riskLevels = null) {
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
    // 이탈(익절/손절) 시점과 가장 가까운 캡처 시각의 인덱스를 찾아 그 위치에 마커 표시
    let exitIdx = null;
    if (exitPoint && exitPoint.at) {
      const exitTime = new Date(exitPoint.at).getTime();
      let bestDiff = Infinity;
      results.forEach((r, i) => {
        const diff = Math.abs(new Date(r.captured_at).getTime() - exitTime);
        if (diff < bestDiff) { bestDiff = diff; exitIdx = i; }
      });
    }
    return renderIntradaySvgDataUri(
      name, labels, prices,
      exitPoint ? { idx: exitIdx, price: exitPoint.price, label: exitPoint.label } : null,
      riskLevels
    );
  } catch {
    return null; // 차트 생성 실패해도 포스팅 자체는 계속 진행
  }
}

// 가격 배열을 부드러운 곡선(카디널 스플라인) + 그라디언트 + 글로우 필터로 그린 SVG data URI 생성
// exitMark: {idx, price, label} - 익절/손절 시점을 별도 마커(세로 점선+가격표)로 표시 (없으면 생략)
function renderIntradaySvgDataUri(name, labels, prices, exitMark = null, riskLevels = null) {
  const W = 720, H = 340;
  const padL = 46, padR = 24, padT = 66, padB = 36;
  const plotW = W - padL - padR;
  const plotH = H - padT - padB;
  const n = prices.length;
  const priceMin = Math.min(...prices);
  const priceMax = Math.max(...prices);
  // 손절/익절 목표선이 오늘 관측된 가격 범위 밖에 있어도 화면 안에 들어오도록 Y축 스케일 범위에는 포함
  const scaleCandidates = [...prices];
  if (riskLevels?.stopLoss != null) scaleCandidates.push(riskLevels.stopLoss);
  if (riskLevels?.takeProfit != null) scaleCandidates.push(riskLevels.takeProfit);
  const min = Math.min(...scaleCandidates);
  const max = Math.max(...scaleCandidates);
  const range = (max - min) || 1;
  const priceToY = (p) => padT + plotH - ((p - min) / range) * plotH;

  const pts = prices.map((p, i) => [
    padL + (i / (n - 1)) * plotW,
    padT + plotH - ((p - min) / range) * plotH
  ]);

  const isUp = prices[n - 1] >= prices[0];
  const changePct = (((prices[n - 1] - prices[0]) / prices[0]) * 100).toFixed(2);
  const color = isUp ? '#FF3B30' : '#0A6CFF';
  const arrow = isUp ? '▲' : '▼';

  const linePath = smoothPath(pts);
  const areaPath = `${linePath} L ${pts[n - 1][0]},${padT + plotH} L ${pts[0][0]},${padT + plotH} Z`;

  let grid = '';
  for (let i = 0; i <= 3; i++) {
    const gy = padT + (plotH / 3) * i;
    grid += `<line x1="${padL}" y1="${gy}" x2="${W - padR}" y2="${gy}" stroke="#F1F2F6" stroke-width="1"/>`;
  }

  const tickIdx = [0, Math.floor((n - 1) / 2), n - 1];
  const xlabels = tickIdx.map(i =>
    `<text x="${pts[i][0]}" y="${H - 10}" font-size="11" fill="#9AA0AC" text-anchor="middle" font-family="sans-serif">${labels[i]}</text>`
  ).join('');

  const maxIdx = prices.indexOf(priceMax);
  const minIdx = prices.indexOf(priceMin);
  // 시작/최고/최저/현재 - 중복 인덱스는 한 번만 표시
  const keyIdx = [...new Set([0, maxIdx, minIdx, n - 1])];
  const fmtPrice = (p) => Math.round(p).toLocaleString('ko-KR');
  const keyPoints = keyIdx.map(i => {
    const [x, y] = pts[i];
    const above = y > padT + 16; // 상단 여백 부족하면 라벨을 점 아래로
    const labelY = above ? y - 12 : y + 20;
    return `<circle cx="${x}" cy="${y}" r="4" fill="#fff" stroke="${color}" stroke-width="2"/>` +
      `<text x="${x}" y="${labelY}" font-size="33" font-weight="700" fill="${color}" text-anchor="middle" font-family="sans-serif">${fmtPrice(prices[i])}</text>`;
  }).join('');

  // 익절/손절 시점 마커: 세로 점선 + 앰버색 마커(라인 색과 확실히 구분) + 라벨
  let exitMarkSvg = '';
  if (exitMark && exitMark.idx != null && pts[exitMark.idx]) {
    const [ex, ey] = pts[exitMark.idx];
    const exColor = '#F5A623';
    const exAbove = ey > padT + 30;
    const exLabelY = exAbove ? ey - 34 : ey + 30;
    const exPriceLabelY = exAbove ? ey - 18 : ey + 46;
    exitMarkSvg = `
<line x1="${ex}" y1="${padT}" x2="${ex}" y2="${padT + plotH}" stroke="${exColor}" stroke-width="1.5" stroke-dasharray="4,3"/>
<circle cx="${ex}" cy="${ey}" r="7" fill="${exColor}" opacity="0.25"/>
<circle cx="${ex}" cy="${ey}" r="6" fill="${exColor}" stroke="#fff" stroke-width="2"/>
<rect x="${ex - 24}" y="${exLabelY - 14}" width="48" height="16" rx="4" fill="${exColor}"/>
<text x="${ex}" y="${exLabelY - 2}" font-size="10" font-weight="700" fill="#fff" text-anchor="middle" font-family="sans-serif">${escapeXml(exitMark.label || '')}</text>
<text x="${ex}" y="${exPriceLabelY}" font-size="14" font-weight="800" fill="${exColor}" text-anchor="middle" font-family="sans-serif">${fmtPrice(exitMark.price)}</text>`;
  }

  // 손절가/익절가 목표 수평선 (이탈 전 종목에도 목표 레벨을 미리 표시)
  let riskLinesSvg = '';
  let riskLabelsSvg = '';
  if (riskLevels?.stopLoss != null) {
    const y = priceToY(riskLevels.stopLoss);
    riskLinesSvg += `\n<line x1="${padL}" y1="${y}" x2="${W - padR}" y2="${y}" stroke="#0A6CFF" stroke-width="1.5" stroke-dasharray="6,4" opacity="0.7"/>`;
    riskLabelsSvg += `\n<rect x="${W - padR - 74}" y="${y - 10}" width="72" height="16" rx="4" fill="#0A6CFF"/>` +
      `<text x="${W - padR - 38}" y="${y + 2}" font-size="10" font-weight="700" fill="#fff" text-anchor="middle" font-family="sans-serif">손절 ${fmtPrice(riskLevels.stopLoss)}</text>`;
  }
  if (riskLevels?.takeProfit != null) {
    const y = priceToY(riskLevels.takeProfit);
    riskLinesSvg += `\n<line x1="${padL}" y1="${y}" x2="${W - padR}" y2="${y}" stroke="#FF3B30" stroke-width="1.5" stroke-dasharray="6,4" opacity="0.7"/>`;
    riskLabelsSvg += `\n<rect x="${W - padR - 74}" y="${y - 10}" width="72" height="16" rx="4" fill="#FF3B30"/>` +
      `<text x="${W - padR - 38}" y="${y + 2}" font-size="10" font-weight="700" fill="#fff" text-anchor="middle" font-family="sans-serif">익절 ${fmtPrice(riskLevels.takeProfit)}</text>`;
  }

  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">
<defs>
<linearGradient id="fillGrad" x1="0" y1="0" x2="0" y2="1">
<stop offset="0%" stop-color="${color}" stop-opacity="0.55"/>
<stop offset="55%" stop-color="${color}" stop-opacity="0.20"/>
<stop offset="100%" stop-color="${color}" stop-opacity="0"/>
</linearGradient>
<linearGradient id="lineGrad" x1="0" y1="0" x2="0" y2="1">
<stop offset="0%" stop-color="#FFFFFF" stop-opacity="0.95"/>
<stop offset="45%" stop-color="${color}" stop-opacity="1"/>
<stop offset="100%" stop-color="${color}" stop-opacity="1"/>
</linearGradient>
<radialGradient id="floorShadow" cx="50%" cy="50%" r="50%">
<stop offset="0%" stop-color="#000000" stop-opacity="0.18"/>
<stop offset="100%" stop-color="#000000" stop-opacity="0"/>
</radialGradient>
<filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
<feGaussianBlur stdDeviation="6" result="blur"/>
<feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
</filter>
<filter id="dropShadow" x="-30%" y="-30%" width="160%" height="160%">
<feDropShadow dx="0" dy="5" stdDeviation="5" flood-color="${color}" flood-opacity="0.4"/>
</filter>
</defs>
<rect width="${W}" height="${H}" fill="#FFFFFF"/>
<ellipse cx="${padL + plotW / 2}" cy="${padT + plotH + 6}" rx="${plotW / 2}" ry="8" fill="url(#floorShadow)"/>
<text x="${padL}" y="30" font-size="18" font-weight="700" fill="${color}" font-family="sans-serif">${escapeXml(name)}  ${arrow} ${changePct}%</text>
<text x="${padL}" y="48" font-size="11" fill="#9AA0AC" font-family="sans-serif">09:00 ~ 현재 · 1분봉</text>
${grid}
${riskLinesSvg}
<path d="${areaPath}" fill="url(#fillGrad)"/>
<path d="${linePath}" fill="none" stroke="${color}" stroke-width="3" stroke-linejoin="round" stroke-linecap="round" filter="url(#glow)" opacity="0.85"/>
<path d="${linePath}" fill="none" stroke="url(#lineGrad)" stroke-width="4" stroke-linejoin="round" stroke-linecap="round" filter="url(#dropShadow)"/>
<circle cx="${pts[n - 1][0]}" cy="${pts[n - 1][1]}" r="6.5" fill="${color}" opacity="0.25"/>
<circle cx="${pts[n - 1][0]}" cy="${pts[n - 1][1]}" r="5.5" fill="#fff" stroke="${color}" stroke-width="2.5" filter="url(#dropShadow)"/>
${keyPoints}
${exitMarkSvg}
${riskLabelsSvg}
${xlabels}
</svg>`;

  return svgToDataUri(svg);
}

// 카디널(캣멀-롬) 스플라인으로 부드러운 곡선 path 생성
function smoothPath(pts) {
  if (pts.length < 2) return `M ${pts[0][0]},${pts[0][1]}`;
  let d = `M ${pts[0][0]},${pts[0][1]}`;
  for (let i = 0; i < pts.length - 1; i++) {
    const p0 = pts[i - 1] || pts[i];
    const p1 = pts[i];
    const p2 = pts[i + 1];
    const p3 = pts[i + 2] || p2;
    const cp1x = p1[0] + (p2[0] - p0[0]) / 6;
    const cp1y = p1[1] + (p2[1] - p0[1]) / 6;
    const cp2x = p2[0] - (p3[0] - p1[0]) / 6;
    const cp2y = p2[1] - (p3[1] - p1[1]) / 6;
    d += ` C ${cp1x},${cp1y} ${cp2x},${cp2y} ${p2[0]},${p2[1]}`;
  }
  return d;
}

function escapeXml(str) {
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// UTF-8(한글 포함) 문자열을 안전하게 base64 data URI로 변환
function svgToDataUri(svg) {
  const bytes = new TextEncoder().encode(svg);
  let binary = '';
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  const base64 = btoa(binary);
  return `data:image/svg+xml;base64,${base64}`;
}

// Google 뉴스 RSS에서 종목명 관련 최신 기사 최대 3건(제목+실제 링크) 조회
async function fetchStockNews(stockName) {
  try {
    const q = encodeURIComponent(`${stockName} 주식`);
    const res = await fetch(`https://news.google.com/rss/search?q=${q}&hl=ko&gl=KR&ceid=KR:ko`);
    if (!res.ok) return [];
    const xml = await res.text();
    const itemMatches = [...xml.matchAll(/<item>([\s\S]*?)<\/item>/g)].slice(0, 3);
    const news = [];
    for (const m of itemMatches) {
      const item = m[1];
      const titleMatch = item.match(/<title>([\s\S]*?)<\/title>/);
      const linkMatch = item.match(/<link>([\s\S]*?)<\/link>/);
      if (!titleMatch || !linkMatch) continue;
      news.push({
        title: titleMatch[1].replace(/<!\[CDATA\[|\]\]>/g, '').trim(),
        link: linkMatch[1].trim()
      });
    }
    return news;
  } catch {
    return []; // 뉴스 조회 실패해도 포스팅 자체는 계속 진행
  }
}

// watchlist_risk_status에서 해당 종목의 손절가/익절가 목표선 조회 (읽기전용, 값 없으면 null)
async function getRiskLevels(env, code) {
  try {
    const row = await env.DB.prepare(
      `SELECT stop_loss, take_profit FROM watchlist_risk_status WHERE code = ? ORDER BY checked_at DESC LIMIT 1`
    ).bind(code).first();
    if (!row || (row.stop_loss == null && row.take_profit == null)) return null;
    return { stopLoss: row.stop_loss, takeProfit: row.take_profit };
  } catch {
    return null;
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
  const listText = stocks.map(s => {
    const newsPart = (s.news && s.news.length)
      ? `, 관련뉴스=[${s.news.map(n => `"${n.title}"`).join(', ')}]`
      : '';
    return `- ${s.name}(${s.code}): 신호=${s.added_state || '-'}, 진입가=${s.entry_price ?? '-'}, 등락률=${s.change_rate != null ? s.change_rate + '%' : '정보없음'}${newsPart}`;
  }).join('\n');

  const prompt = `너는 15년 경력의 한국 주식시장 애널리스트야. 아래는 실시간 조건검색으로 방금 포착된 급등 신호 종목 리스트야.
증권사 데일리 노트처럼 짧고 명쾌하게 작성해줘. 미사여구, 장황한 배경 설명 없이 핵심만.

[포착 종목]
${listText}

요구사항:
- 제목: 간결하고 핵심이 드러나는 한 줄 (종목명 포함)
- 도입부: 1~2문장으로 오늘 포착 종목군의 공통 특징만 짧게
- 종목별 섹션(각 종목마다 <h3> 소제목으로 "종목명(코드)" 구분):
  아래 세 가지를 각각 별도의 <p> 문단으로 나눠서 작성 (한 문단에 다 몰아넣지 말 것):
  1문단) 포착 신호(added_state)와 현재 가격 흐름(등락률) 한 줄 요약
  2문단) 관련뉴스가 있으면 제목을 자연스럽게 한 문장에 녹여서 언급 (링크는 절대 만들지 마, 별도로 삽입됨)
  3문단) 체크포인트 한 가지만 간결하게
  각 문단은 1문장, 불필요한 수식어·반복 설명 금지. 애널리스트가 시간 없을 때 쓰는 메모 톤
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
    max_tokens: 3500
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
  // 본문 최상단에 정확한 발행 시각(시:분:초까지) 명시
  content = `<p style="font-size:12px;color:#9AA0AC;margin-bottom:14px;">⏱ 발행: ${formatKstTimestamp()}</p>` + content;

  return { title: parsed.title, content };
}

// 발행 시각을 "YYYY-MM-DD HH:MM:SS (KST)" 형식으로 반환
function formatKstTimestamp() {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Seoul',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  }).formatToParts(new Date()).reduce((acc, p) => { acc[p.type] = p.value; return acc; }, {});
  return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second} (KST)`;
}

// 09:00~현재 실제 가격으로 그린 정적 차트 이미지를 종목 섹션 하단에 삽입 (없으면 스킵)
function injectCharts(html, stocks) {
  let result = html;
  for (const s of stocks) {
    if (!s.chartUrl) continue;
    const chartHtml = `<div style="margin:0 0 20px;border:1px solid #E4E7EC;border-radius:8px;overflow:hidden;padding:8px;background:#fff;"><img src="${s.chartUrl}" alt="${escapeHtml(s.name)} 09시~현재 1분봉 차트" style="width:100%;height:auto;display:block;"></div>`;
    const afterNewsPattern = new RegExp(`(<h3[^>]*>[\\s\\S]*?${escapeRegex(s.name)}[\\s\\S]*?\\(${escapeRegex(s.code)}\\)[\\s\\S]*?</h3>(?:\\s*<p[^>]*>📰[\\s\\S]*?</p>\\s*<ul[^>]*>[\\s\\S]*?</ul>)?)`);
    result = result.replace(afterNewsPattern, `$1${chartHtml}`);
  }
  return result;
}

// h3 종목 섹션 바로 뒤에 실제 뉴스 링크 목록(제목 그대로, 실제 URL)을 삽입
function injectNewsLinks(html, stocks) {
  let result = html;
  for (const s of stocks) {
    if (!s.news || !s.news.length) continue;
    const items = s.news.map(n =>
      `<li><a href="${n.link}" target="_blank" rel="noopener">${escapeHtml(n.title)}</a></li>`
    ).join('');
    const newsHtml = `<p style="font-size:13px;color:#6B7280;margin:-4px 0 6px;">📰 관련뉴스</p><ul style="font-size:13px;color:#6B7280;margin:0 0 14px;padding-left:20px;">${items}</ul>`;
    const h3Pattern = new RegExp(`(<h3[^>]*>[\\s\\S]*?${escapeRegex(s.name)}[\\s\\S]*?\\(${escapeRegex(s.code)}\\)[\\s\\S]*?</h3>)`);
    result = result.replace(h3Pattern, `$1${newsHtml}`);
  }
  return result;
}

function escapeHtml(str) {
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// 종목명(코드) h3 소제목 앞에 컬러 원형 배지(로고 대용) 삽입 - 외부 이미지 의존 없이 항상 정상 표시
// + h3 클릭 시 종목코드 클립보드 복사 후 영웅문 앱 실행 시도
function injectLogoBadges(html, stocks) {
  let result = html;
  for (const s of stocks) {
    const initial = (s.name || '').slice(0, 1);
    const color = hashToColor(s.name || s.code);
    const badge = `<span style="display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;border-radius:50%;background:${color};color:#fff;font-size:13px;font-weight:800;margin-right:8px;vertical-align:middle;">${initial}</span>`;
    const hint = `<span style="font-size:11px;font-weight:400;color:#9AA0AC;margin-left:8px;">(탭하여 코드 복사·영웅문 실행)</span>`;
    const onclick = buildKiwoomLaunchJs(s.code);
    const pattern = new RegExp(`<h3([^>]*)>(\\s*${escapeRegex(s.name)}\\s*\\(${escapeRegex(s.code)}\\))`, 'g');
    result = result.replace(pattern, `<h3$1 onclick="${onclick}" style="cursor:pointer;">${badge}$2${hint}`);
  }
  return result;
}

// 클릭 시: 종목코드 클립보드 복사 → 영웅문 앱 실행
// heromts://heromtshost 는 kiwoomapi 프로젝트에서 실제 APK 매니페스트를 직접 까서 확인한
// 진짜 등록된 스킴 (intent:// 패키지 실행 방식은 실기기에서 전부 실패했었음 - 검증 완료된 값)
// 종목코드로 특정 화면 이동은 안 됨(앱만 켜짐) - 코드는 클립보드 복사로 대신 전달
function buildKiwoomLaunchJs(code) {
  const js =
    `try{navigator.clipboard.writeText('${code}')}catch(e){};` +
    `location.href='heromts://heromtshost'`;
  return js.replace(/"/g, '&quot;');
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
    body: JSON.stringify({ title, content, published: nowKstIso() })
  });
  if (!res.ok) {
    throw new Error(`Blogger API error: ${res.status} ${await res.text()}`);
  }
  return res.json();
}

// 현재 시각을 KST(+09:00) 기준 ISO 8601 문자열로 반환 - Blogger 게시일이 다른 타임존으로
// 처리돼 날짜가 하루 어긋나는 것을 방지하기 위해 명시적으로 지정
function nowKstIso() {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Seoul',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  }).formatToParts(new Date()).reduce((acc, p) => { acc[p.type] = p.value; return acc; }, {});
  return `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}+09:00`;
}
