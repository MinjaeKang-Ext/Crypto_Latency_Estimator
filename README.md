# Binance / Hyperliquid latency collection

도쿄 EC2에서 두 거래소 시세를 수집하고, 종료된 파일을 S3로 업로드하는 연구 프로젝트. 모든 시세 수집은 공개 WebSocket이며 계정 키가 필요하지 않습니다. 원본 주문 지연 코드도 `collectors/`에 보관하지만 **자동 실행하지 않습니다**.

## 구조

`거래소 WebSocket → EC2 배치별 파일 → S3 버킷/latency/{거래소}/{UTC시각-랜덤ID}/`

- `collectors/hl_marketdata_latency.py`: HL mainnet L2/BBO/trades, 기본 5호가.
- `collectors/binance_usdm_feed_latency.py`: Binance USD-M mainnet L2 변경분/BBO/집계 체결.
- `collect_and_upload.py`: 수집 종료 후 CSV·원본 JSONL·요약·메타데이터·로그 업로드.
- `collectors/hl_mainnet_latency.py` 및 `collectors/binance_order_latency.py`: 별도 주문 지연 실험. 인증·환경 설정 후 사용하며 실제 자금이 사용될 수 있음.

## 시작 전

1. AWS 도쿄 리전 EC2와 S3 버킷을 준비합니다. 버킷 공개 접근 차단을 유지하고, 필요에 맞게 수명주기와 예산 알림을 설정합니다. 저장량 및 요청/전송량에 따른 요금이 발생합니다.
2. `iam-s3-policy.example.json`의 `YOUR_BUCKET`을 바꿔 IAM 정책으로 만들고, EC2 인스턴스 역할에 연결합니다. 기본 S3 암호화(SSE-S3) 기준입니다. 고객 관리 KMS 키 사용 시 추가 KMS 권한이 필요합니다. AWS 액세스 키를 저장소에 넣지 않습니다.
3. EC2에서 Python 3.10+, AWS CLI v2, git을 설치합니다. 인스턴스 역할의 자격 증명이 유효한지 `aws sts get-caller-identity`로 확인합니다.

## 설치 및 실행

```bash
git clone https://github.com/MinjaeKang-Ext/Crypto_Latency_Estimator.git
cd Crypto_Latency_Estimator
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

10분간 한 번 수집합니다. `MY_BUCKET`은 실제 버킷명으로 바꾸세요. 심볼 유효성은 각 거래소 환경에서 실행 시 확인되며 실패해도 다른 종목으로 대체하지 않습니다.

```bash
python collect_and_upload.py --venue binance --symbol SKHYNIXUSDT --seconds 600 --bucket MY_BUCKET
python collect_and_upload.py --venue hyperliquid --symbol xyz:SKHX --seconds 600 --bucket MY_BUCKET
```

양쪽을 동시에 수집하려면 EC2에서 터미널 2개를 열어 각 명령을 실행합니다. 먼저 30초로 검증하고 정상 작동 시 600초로 변경하세요. `--forever`를 추가하면 배치를 반복합니다. **배치 사이에는 연결 재설정과 업로드에 따른 공백이 존재**하므로 연속 무손실 수집으로 해석하면 안 됩니다. `--forever`는 터미널 종료 후 자동 지속을 보장하지 않습니다. 장기 운영에는 systemd 등 서비스 관리와 모니터링이 필요합니다.

종료된 로컬 파일은 `data/`에 남고 S3에도 복사됩니다. 업로드가 실패하면 성공 표시를 만들지 않고 중단합니다. 재실행 시 미완료 배치를 먼저 재시도합니다. 프로세스 강제종료나 비정상 종료로 `run.json`이 생성되지 않은 배치는 수동 확인이 필요합니다. 파일 전체가 업로드되기 전에도 일부 객체가 S3에 보일 수 있으므로 분석 전 `run.json`의 `status`를 확인하세요. `completed_with_missing_streams`는 일부 스트림이 비었음을 뜻하며, 거래 부재일 수도 있습니다.

## 지정가 주문·취소 지연 측정

주문 실험은 시세 수집과 별도로 실행합니다. 지정가를 제출하고 접수 응답 직후 취소합니다. `collect_and_upload.py`는 시세 데이터 전용이며 주문을 넣지 않습니다.

| 거래소 | 파일 | 현재 검증 상태 |
| --- | --- | --- |
| Binance USDⓈ-M | `collectors/binance_order_latency.py` | 기본 테스트넷·읽기 전용. 오프라인 테스트만 수행했고 실제 API 연동은 미검증 |
| Hyperliquid | `collectors/hl_mainnet_latency.py` | 메인넷용. 과거 취소 상태 조회 오류가 있어 수정·실거래 검증 전 실행하지 말 것 |

### Binance 테스트넷

먼저 프로젝트 루트에서 가상환경을 활성화하고, 주문 없이 연결과 주문 조건을 확인합니다.

```bash
source .venv/bin/activate
python collectors/binance_order_latency.py --env testnet --symbol BTCUSDT --notional 110 --max-notional 250
```

실험에는 Demo/Futures 테스트넷의 HMAC API Key와 Secret이 필요합니다. 터미널에 입력한 값은 표시되지 않습니다. 테스트넷과 메인넷 키를 섞지 말고, 키나 명령 결과를 공유하지 마세요.

```bash
read -r -s -p "Testnet API Key: " BINANCE_TESTNET_API_KEY
echo
read -r -s -p "Testnet API Secret: " BINANCE_TESTNET_API_SECRET
echo
export BINANCE_TESTNET_API_KEY BINANCE_TESTNET_API_SECRET
```

테스트넷 모의 주문을 한 번만 제출하고 취소합니다. `--out`은 기존에 없는 폴더여야 합니다. `--live`는 선택한 환경에서 주문을 제출한다는 뜻이며, `--env mainnet --live`는 **실제 주문**입니다.

```bash
python collectors/binance_order_latency.py --env testnet --symbol BTCUSDT --notional 110 --max-notional 250 --samples 1 --live --out data/order_binance_test_01
```

`resting_then_canceled_no_fill`로 끝나고 미체결 주문·포지션이 없는 것을 확인한 뒤 100회 실험할 수 있습니다.

```bash
python collectors/binance_order_latency.py --env testnet --symbol BTCUSDT --notional 110 --max-notional 250 --samples 100 --pause 2 --live --out data/order_binance_test_100
```

기본 매수가격은 최우선 매수호가보다 100bp 또는 5틱 중 더 먼 가격입니다. GTX는 즉시 체결만 방지하며, 이후 가격 변동에 따른 체결은 가능합니다. 오류나 중단 시 미체결 주문과 포지션을 확인하세요. 자동 정리 기능은 체결된 포지션을 닫지 않습니다.

출력 폴더에 `latency.csv`, `summary.json`, `raw.jsonl`, `run.json`이 생성됩니다. `ack_ms`와 `open_ms`는 신규 주문 전송부터 각 응답과 NEW 이벤트까지의 시간, `cancel_ack_ms`와 `canceled_ms`는 **취소 전송부터** 각 취소 응답과 CANCELED 이벤트까지의 시간입니다. 이벤트와 API 응답의 도착 순서는 달라질 수 있습니다.

주문 실험 결과는 S3에 **자동 업로드되지 않습니다**. 직접 저장하려면 버킷과 폴더명을 지정해 업로드합니다. 주문 이벤트에는 계정 관련 정보가 포함될 수 있으므로 접근이 제한된 버킷을 사용하세요.

```bash
aws s3 cp data/order_binance_test_01/ s3://MY_BUCKET/latency/order-tests/binance/test_01/ --recursive
```

### Hyperliquid 주문 테스트

기존 코드는 `--live` 없이 실행하면 주문을 보내지 않는 점검 모드입니다. 과거 취소 확인 오류가 관찰됐으므로 `--live`를 사용하기 전에 코드를 수정하고 안전하게 재검증해야 합니다.

```bash
python collectors/hl_mainnet_latency.py --korea-stock skhynix --offset-ticks 500 --samples 1
```

## 검증

```bash
aws s3 ls s3://MY_BUCKET/latency/ --recursive
cat data/binance/*/run.json
cat data/hyperliquid/*/run.json
```

`latency.csv`, `raw.jsonl`, `summary.json`, `run.json`, `collector.log`가 각 배치에 저장됩니다. 키·지갑 정보와 개인 계정 데이터는 저장소에 올리지 마세요. `.gitignore`는 보호에 도움이 되지만 이미 추적한 파일이나 시크릿을 자동으로 제거하지는 않습니다.

## 해석 및 제한

- 바이낸스 L2는 증분 업데이트, HL L2는 호가 스냅샷입니다. HL 체결은 개별 거래, 바이낸스는 aggTrade 집계 메시지로 집계 단위가 다릅니다.
- 이벤트 시각과 로컬 수신 시각의 차이는 거래소 시계·로컬 시계 오차, 내부 처리 및 프로그램 지연을 포함합니다. 순수 네트워크 편도 지연이나 주문 처리 지연으로 단정할 수 없습니다.
- 배치 중 연결 오류 시 자동 복구하며 이어 붙이지 않습니다. 미완성 결과와 기록은 보존하고 중단합니다.
- 과거 HL 주문 측정에서 취소 직후 REST 상태가 `open`으로 보이는 문제가 관찰됐습니다. 해당 코드를 수정·실거래 검증하기 전 주문 실험에 쓰지 마세요. Binance 주문 코드는 오프라인 테스트만 통과했으며 실제 API/거래 연동은 미검증입니다.
- 이 리포지토리는 AWS 계정 리소스를 생성하거나 EC2 서비스를 자동 배포하지 않습니다. S3 실업로드는 사용자의 계정과 인스턴스 역할 설정 후 검증해야 합니다.
