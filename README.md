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
