# Solana Memecoin Trading Bot · Kiến trúc, cấu hình và vận hành

Tài liệu này mô tả hệ thống **Solana Memecoin Quantitative Trading Bot & Real-Time Dashboard**. Backend hiện hỗ trợ Paper Trading và auto-execution LIVE bằng keypair cục bộ. Bitget Wallet hiện mới có endpoint dựng unsigned swap và tham chiếu địa chỉ ví; luồng ký/gửi tự động qua extension chưa được tích hợp end-to-end. Không xem đây là hệ thống production-audited.

## 1. Tổng Quan Kiến Trúc Hệ Thống (System Architecture Overview)

Hệ thống được thiết kế theo mô hình **Event-Driven Modular Architecture** kết hợp giữa **Python 3.11+ (`asyncio` + `FastAPI` + `WebSockets`)** ở tầng Backend và **Single-Page Web Dashboard (`HTML5` + `Tailwind CSS` + `Chart.js`)** ở tầng Frontend.

| Thành phần (Module) | Công nghệ & Giao thức | Nguồn API Miễn Phí 100% | Chức năng cốt lõi |
| :--- | :--- | :--- | :--- |
| **Token Discovery (`scanner.py`)** | `websockets`, `aiohttp` | [PumpPortal WebSocket API](https://pumpportal.fun/data-api/real-time/) & [DEX Screener API](https://docs.dexscreener.com/api/reference) | Lắng nghe sự kiện tạo token mới (`subscribeNewToken`) và hoàn tất bonding curve (`subscribeMigration`) trên Pump.fun/Raydium; lọc vốn hóa **$150,000 – $500,000** (300 req/phút miễn phí). |
| **Security & Rugcheck (`security.py`)** | `aiohttp`, Solana JSON-RPC | [RugCheck API v1](https://api.rugcheck.xyz/swagger/index.html) & [Solana Public RPC](https://solana.com/docs) | Truy vấn `/v1/tokens/{mint}/report` và kiểm tra trực tiếp trên chuỗi: Mint Authority đã khóa (`null`), Freeze Authority đã tắt (`null`), LP đã đốt/khóa (`>= 95%`), và ví Top 10 ngoài LP (`<= 12%/ví`, tổng `<= 30%`). |
| **Quant Strategy (`strategy.py`)** | `numpy`, `pandas` | Dữ liệu nến & sổ lệnh từ [DEX Screener API](https://docs.dexscreener.com/api/reference) | Phân tích khung 1m và 5m: Tỷ lệ Volume Mua/Bán (`>= 1.6x`), Tốc độ người mua mới (`>= 15 ví/5m`), Tỷ lệ Thanh khoản/MCap (`>= 8%`), RSI-14 (`45–72`), giá nằm trên VWAP 5m kèm đột biến khối lượng (`>= 1.8x`). Quản lý máy trạng thái TP đa tầng (`+50%`, `+100%`), Hard SL (`-20%`) và Dynamic Trailing Stop (kích hoạt tại `+25%`, trượt `12%` dưới đỉnh). |
| **DEX Execution (`executor.py`)** | `solders`, `aiohttp` | [Jupiter Swap API v1](https://developers.jup.ag/docs/swap/v1/get-quote), [Helius Sender](https://www.helius.dev/docs/sending-transactions/sender) | Lấy quote Jupiter, dựng swap transaction, ký/gửi LIVE bằng keypair cục bộ ở backend; hỗ trợ dựng unsigned swap bundle để tích hợp ví ở một luồng riêng. Ghi log JSONL và CSV. |
| **Backtest Engine (`backtester.py`)** | `asyncio`, `numpy` | [GeckoTerminal Public API v2](https://api.geckoterminal.com/docs/index.html) & [CoinGecko Keyless API](https://docs.coingecko.com/docs/keyless-public-api) | Tải dữ liệu lịch sử OHLCV 1m/5m miễn phí từ `/api/v2/networks/solana/pools/{pool}/ohlcv/minute` hoặc chạy Paper Trading thời gian thực không tốn SOL thật; tính toán Win Rate, Net PnL (SOL/USD), Max Drawdown, Sharpe Ratio và Profit Factor. |
| **Web Dashboard (`dashboard/`)** | `Tailwind CSS`, `Chart.js`, FastAPI REST/WebSocket | Backend nội bộ | Giao diện Dark-Mode gồm Profit, Execution Terminal và cấu hình bot; dữ liệu đọc từ SQLite và sự kiện WebSocket. Dashboard hiện không kết nối/ký giao dịch bằng Bitget extension. |

### Cấu Trúc Thư Mục Dự Án

```text
solana-memecoin-bot/
├── .env.example                 # Biến môi trường mẫu (100% dùng endpoint miễn phí)
├── config.json                  # Cấu hình chiến lược, bộ lọc MCap ($150k-$500k), TP/SL, Trailing Stop
├── requirements.txt             # Thư viện Python (fastapi, uvicorn, aiohttp, websockets, solders, numpy)
├── Dockerfile                   # Đóng gói container nhẹ (Python 3.11-slim)
├── docker-compose.yml           # Chạy toàn bộ hệ thống bằng 1 lệnh duy nhất
├── scanner.py                   # Module 1: Quét Pump.fun (PumpPortal WS) & lọc Market Cap (DexScreener)
├── security.py                  # Module 2: Kiểm tra Rugcheck.xyz API & xác thực On-Chain (Mint/Freeze/LP/Holders)
├── strategy.py                  # Module 3: Chỉ báo định lượng (RSI, VWAP, Vol Ratio) & State Machine TP/SL/Trailing
├── executor.py                  # Module 4: Thực thi lệnh qua Jupiter API + Helius/Jito + Bitget Wallet Unsigned Tx
├── backtester.py                # Module 5: Động cơ Backtest lịch sử (GeckoTerminal OHLCV) & Paper Trading
├── server.py                    # Module 6: FastAPI + WebSocket Server kết nối Backend với Dashboard
├── logs/                        # Lưu trữ tự động structured_events.jsonl và trades_history.csv
└── dashboard/
    └── index.html               # Dashboard gồm Profit, Execution Terminal và Cấu hình Bot
```

## 2. Trạng thái tích hợp Bitget Wallet

- `BITGET_WALLET_PUBLIC_KEY` chỉ cung cấp địa chỉ tham chiếu khi backend không có keypair; nó không cấp quyền ký giao dịch.
- `POST /api/bitget/build-swap` có thể dựng unsigned Jupiter swap transaction. Tuy nhiên dashboard hiện tại chưa triển khai kết nối provider, chuyển transaction tới extension, nhận chữ ký và xác nhận kết quả.
- Auto-execution LIVE hiện chỉ hoạt động khi backend nạp được `SOLANA_PRIVATE_KEY_BASE58`. Hãy dùng ví phụ có số dư giới hạn, không dùng ví lưu trữ chính.
- Không bao giờ nhập seed phrase/private key vào dashboard, file được chia sẻ hoặc cuộc trò chuyện.


## 3. Chi Tiết Luật Định Lượng & Quản Trị Rủi Ro

### Bộ Lọc Sàn Lọc Đa Tầng (4-Stage Filtering Pipeline)

| Tầng kiểm duyệt | Điều kiện bắt buộc (Mặc định trong `config.json`) | Nguồn dữ liệu xác thực | Lý do loại bỏ (Rejection Reason) |
| :--- | :--- | :--- | :--- |
| **1. Market Cap Filter** | `$150,000 <= market_cap_usd <= $500,000` | [DEX Screener API](https://docs.dexscreener.com/api/reference) | Loại bỏ các token mới tạo quá non (`<$150k`, dễ sập bonding curve) hoặc đã tăng quá cao (`>$500k`, biên lợi nhuận thấp). |
| **2. Rugcheck.xyz Gate** | `risk_level != "Danger"` và `normalized_score <= 400` | [RugCheck API v1](https://api.rugcheck.xyz/swagger/index.html) | Từ chối các hợp đồng có cảnh báo honeypot, phí chuyển ẩn, hoặc dev liên kết ví xả. |
| **3. On-Chain Security** | `mint_authority == null`, `freeze_authority == null`, `lp_burned_pct >= 95%`, `max_single_holder_pct <= 12%`, `top10_holders_pct <= 30%` | [Solana RPC](https://solana.com/docs) & [RugCheck Report](https://cryptouniversity.network/guides/rugcheck-api-guide-how-to-scan-solana-tokens-programmatically) | Chống in thêm token (Mint), chống đóng băng ví người mua (Freeze), chống rút thanh khoản (Rug LP) và chống cá voi/dev xả (Dev Dump). |
| **4. Quantitative Trigger** | `buy_sell_vol_ratio_5m >= 1.6`, `unique_buyers_5m >= 15`, `liq_to_mcap_ratio >= 0.08`, `45 <= RSI_14 <= 72`, `price > VWAP_5m`, `vol_spike_1m >= 1.8x` | [DEX Screener Pairs API](https://docs.dexscreener.com/api/reference) | Đảm bảo dòng tiền mua thực sự áp đảo lực bán, thanh khoản đủ dày để không bị trượt giá nặng và động lượng tăng bền vững. |

### Máy Trạng Thái Chốt Lời / Cắt Lỗ & Trailing Stop (`strategy.py`)

- **Take Profit Tầng 1 (TP1):** Khi lợi nhuận chưa thực hiện đạt **`+50%`**, tự động bán **`50%`** khối lượng vị thế ban đầu để thu hồi phần lớn vốn gốc.
- **Take Profit Tầng 2 (TP2):** Khi lợi nhuận chưa thực hiện đạt **`+100%`** (x2), tự động bán tiếp **`25%`** khối lượng vị thế ban đầu (tương đương 50% lượng còn lại).
- **Dynamic Trailing Stop (Chốt lời động theo đỉnh):**
  - Kích hoạt ngay khi lợi nhuận chạm mốc **`+25%`** (`trailing_activation_pct = 25.0`).
  - Liên tục ghi nhận mức giá đỉnh cao nhất `highest_price = max(highest_price, current_price)`.
  - Ngưỡng kích hoạt bán động được cập nhật theo công thức:
    $$\text{Trailing Trigger Price} = \text{Highest Price} \times \left(1 - \frac{\text{Trailing Distance \%}}{100}\right)$$
  - Với `trailing_distance_pct = 12.0%`, nếu giá token tăng lên `+140%` rồi điều chỉnh giảm `12%` từ đỉnh xuống `+111.2%`, bot sẽ lập tức đóng toàn bộ phần vị thế còn lại để khóa lợi nhuận.
- **Hard Stop Loss (SL cứng):** Nếu giá giảm chạm **`-20%`** so với giá vào lệnh (`stop_loss_pct = -20.0`) trước khi kích hoạt Trailing Stop, bot lập tức đóng 100% vị thế để bảo toàn vốn.

## 4. Hướng Dẫn Chạy & Deploy Miễn Phí 100% (0 Đồng Chi Phí Duy Trì)

Toàn bộ kiến trúc được thiết kế để bạn **không phải trả bất kỳ chi phí thuê bao API hay máy chủ nào**. Dưới đây là bảng kiểm tra các dịch vụ miễn phí được sử dụng và 3 cách triển khai hoàn toàn miễn phí.

### Bảng Xác Thực Các API Miễn Phí 100% Trong Hệ Thống

| Dịch vụ | Endpoint sử dụng trong Code | Hạn mức Miễn phí (Free Tier) | Yêu cầu Thẻ tín dụng? |
| :--- | :--- | :--- | :--- |
| **PumpPortal Data Stream** | `wss://pumpportal.fun/api/data` (`subscribeNewToken`, `subscribeMigration`) | Miễn phí không giới hạn cho luồng tạo token & migration (dùng 1 kết nối WebSocket duy nhất theo [PumpPortal Real-time Docs](https://pumpportal.fun/data-api/real-time/)). | **Không** |
| **DEX Screener API** | `https://api.dexscreener.com/latest/dex/tokens/{mint}` | Miễn phí **300 requests/phút** cho dữ liệu cặp giao dịch & Market Cap theo [DEX Screener Reference](https://docs.dexscreener.com/api/reference). | **Không** |
| **Rugcheck.xyz API** | `https://api.rugcheck.xyz/v1/tokens/{mint}/report` | Miễn phí truy vấn báo cáo rủi ro token công khai theo [RugCheck Swagger UI](https://api.rugcheck.xyz/swagger/index.html). | **Không** |
| **GeckoTerminal OHLCV API** | `https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool}/ohlcv/minute` | Miễn phí **30 requests/phút** lấy nến lịch sử phục vụ Backtesting theo [GeckoTerminal API Docs](https://api.geckoterminal.com/docs/index.html). | **Không** |
| **Jupiter Swap & Price API** | `https://lite-api.jup.ag/swap/v1/quote` & `/swap` | Miễn phí hoàn toàn, không thu phí giao thức cho lệnh swap cơ bản theo [Jupiter Swap Docs](https://developers.jup.ag/docs/swap/v1/get-quote). | **Không** |
| **Solana RPC & Helius Sender** | `https://api.mainnet-beta.solana.com` hoặc [Helius Free Tier](https://docs.rpcplane.dev/providers/helius/) + `https://sender.helius-rpc.com/fast` | Helius miễn phí **1,000,000 credits/tháng** (10 req/giây) và tính năng [Helius Sender](https://www.helius.dev/docs/sending-transactions/sender) miễn phí **0 credit** trên mọi gói. | **Không** |
| **Bitget Wallet** | `window.bitkeep.solana` | Tiện ích mở rộng trình duyệt miễn phí theo [Bitget Wallet Solana Docs](https://web3.bitget.com/en/docs/connect/mainnet/solana). | **Không** |

### Cách 1: Chạy Trực Tiếp Trên Máy Tính Cá Nhân (Khuyên Dùng – Nhanh, Bảo Mật & Free 100%)

Khi chạy trên máy cá nhân (Windows / macOS / Linux), khóa ký LIVE được giữ trong file `.env` cục bộ và backend mặc định chỉ bind vào `127.0.0.1`.

1. **Giải nén gói mã nguồn `solana_memecoin_bot_full_stack.zip`** và mở terminal tại thư mục dự án:
   ```bash
   unzip solana_memecoin_bot_full_stack.zip -d solana-memecoin-bot
   cd solana-memecoin-bot
   ```
2. **Tạo môi trường ảo Python và cài đặt thư viện miễn phí**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # Trên Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. **Thiết lập file cấu hình `.env`**:
   ```bash
   cp .env.example .env
   ```
   - Mặc định `.env` đã cấu hình sẵn `TRADING_MODE=PAPER` (chạy giả lập thời gian thực an toàn 100%) và sử dụng Public Solana RPC miễn phí.
   - *(Tùy chọn)* Để tăng tốc độ RPC miễn phí, bạn có thể đăng ký tài khoản miễn phí bằng email hoặc ví Solana tại [Helius Developer Portal](https://www.helius.dev/) (không cần thẻ tín dụng, tặng 1 triệu credits/tháng) và dán URL vào `SOLANA_RPC_URL`.
4. **Khởi chạy Bot Server + Web Dashboard**:
   ```bash
   python3 server.py
   ```
5. **Mở Dashboard**:
   - Truy cập `http://localhost:8000`.
   - Dashboard lấy dữ liệu từ backend; Bitget extension chưa được nối vào luồng ký lệnh tự động trong bản này.

### Cách 2: Chạy Bằng Docker Compose (1 Lệnh Duy Nhất – Free 100%)

Nếu máy tính hoặc VPS của bạn đã cài đặt Docker:

```bash
cp .env.example .env
docker compose up --build -d
```

- Xem log trực tiếp: `docker compose logs -f`
- Truy cập Web Dashboard tại: `http://localhost:8000`

### Cách 3: Deploy Lên Cloud 24/7 Miễn Phí 100% (Render / Koyeb / Oracle Cloud Free + Cloudflare Pages / Vercel)

Nếu bạn muốn treo bot quét thị trường 24/7 trên đám mây mà không tốn chi phí thuê VPS:

1. **Tùy chọn Cloud A — Render.com (Web Service Free Tier) hoặc Koyeb (Free Nano Service)**:
   - Đẩy thư mục mã nguồn (trừ file `.env`) lên một **Private GitHub Repository**.
   - Đăng nhập vào **Render.com** hoặc **Koyeb.com** (hỗ trợ đăng nhập bằng GitHub, gói Free không tốn phí).
   - Chọn **New Web Service** -> Kết nối repo GitHub của bạn -> Chọn môi trường **Docker** (hoặc **Python 3** với Build Command: `pip install -r requirements.txt` và Start Command: `uvicorn server:app --host 0.0.0.0 --port $PORT`).
   - Trong mục **Environment Variables**, thêm các biến từ `.env.example`. Không bật LIVE trên cloud công cộng nếu chưa có mô hình quản lý secrets, giới hạn truy cập và giám sát bảo mật phù hợp.
   - Để giữ dịch vụ Render Free không bị ngủ sau 15 phút không tương tác, bạn có thể dùng dịch vụ ping miễn phí như **cron-job.org** gọi vào endpoint `https://<ten-app-cua-ban>.onrender.com/api/health` mỗi 10 phút.
2. **Tùy chọn Cloud B — Oracle Cloud Infrastructure (OCI Always Free ARM/AMD VM)**:
   - Nếu bạn muốn một máy chủ Linux chạy 24/7 không bao giờ ngủ: gói **Always Free** của Oracle Cloud cung cấp miễn phí vĩnh viễn 2 máy ảo AMD Micro hoặc 4 OCPU / 24GB RAM Ampere ARM.
   - Chỉ cần SSH vào máy ảo, chạy `git clone`, `docker compose up -d` là bot sẽ hoạt động 24/7 hoàn toàn miễn phí.
3. **Tùy chọn Deploy riêng Frontend Dashboard lên Cloudflare Pages / Vercel / GitHub Pages (Free 100%)**:
   - Dashboard hiện gọi REST/WebSocket cùng origin với FastAPI. Không deploy riêng file HTML lên static hosting nếu chưa cấu hình API proxy, HTTPS, authentication và giới hạn quyền truy cập phù hợp.


## 10. Live Dashboard UI (bản tinh gọn)

Dashboard Live được tinh gọn thành đúng ba khu vực hiển thị:
- **Profit:** biểu đồ PnL/equity và bộ lọc thời gian.
- **Lệnh đã thực hiện (Terminal):** nhật ký thực thi, lọc sự kiện và danh sách coin đã giao dịch.
- **Cấu hình Bot:** bộ lọc Market Cap, Rugcheck, holder, volume, khối lượng lệnh, TP/SL và trailing stop.

Các module scanner, security, strategy, executor, backtester, database và API server được giữ nguyên trong gói mã nguồn. Những tab phân tích/backtest và các bảng phụ không còn xuất hiện trong giao diện Live chính; mã nguồn vẫn được giữ lại để không làm mất các luồng xử lý hiện có.


## Ghi chú triển khai quan trọng (bản hoàn thiện dashboard)

- Dashboard hiện chỉ hiển thị ba khu vực: **Profit**, **Lệnh đã thực hiện (Terminal)** và **Cấu hình Bot**. Dữ liệu được tải từ FastAPI/SQLite; không còn số liệu mẫu, lệnh demo hay ticker PnL ngẫu nhiên.
- Mặc định nên chạy `PAPER`. Chỉ chọn `LIVE` sau khi kiểm tra ví phụ, số dư, slippage, RPC, giới hạn lệnh và toàn bộ cấu hình. LIVE hiện ký giao dịch bằng `SOLANA_PRIVATE_KEY_BASE58` ở backend. Không đưa private key/seed phrase vào trình duyệt hoặc gửi qua chat.
- **Bitget Wallet Extension chưa được tích hợp end-to-end cho bot tự động.** `build-unsigned-swap` chỉ tạo giao dịch unsigned; dashboard hiện tại không thực hiện vòng đời ký/gửi bằng extension. `BITGET_WALLET_PUBLIC_KEY` chỉ được dùng làm địa chỉ tham chiếu nếu không có keypair, không phải bằng chứng bot có thể ký lệnh bằng Bitget.
- Cấu hình `min_unique_buyers_5m` trong chiến lược hiện đang được so với số lượng giao dịch BUY 5m do DEX Screener trả về; đó không phải số ví mua duy nhất. Cần một nguồn dữ liệu on-chain/trades phù hợp trước khi coi tiêu chí này là unique wallets.
- Chỉ báo entry không còn tự sinh nến giả khi GeckoTerminal thiếu dữ liệu: token bị bỏ qua cho tới khi có đủ tối thiểu 15 nến 1m.
- Dashboard dùng Chart.js và Tailwind CDN, vì vậy trình duyệt cần Internet để tải thư viện giao diện. Backend vẫn chạy cục bộ.

### Chạy an toàn

```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# Giữ TRADING_MODE=PAPER trong giai đoạn kiểm thử
python server.py
```

Mở `http://localhost:8000`. API điều khiển bot: `POST /api/bot/start?mode=PAPER`, `POST /api/bot/stop`, `GET /api/state`, `POST /api/config`. Trước khi dùng LIVE, hãy thử Paper đủ lâu và kiểm tra log/transaction trên Solscan. Không có chiến lược nào bảo đảm tỷ lệ thắng hoặc lợi nhuận.
