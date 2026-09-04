---
type: ssot
owner: governance-team
last_updated: 2026-09-03
---

# bus-foundation — System Boundary

> 本文档描述 bus-foundation 与 eCOS 系统其他部分的边界：暴露的接口、依赖的上游、影响的下游。
>
> 系统全景参见：[`../../docs/PANORAMA.md`](../../docs/PANORAMA.md)

---

## 1. 暴露接口

### BOS URI



### 入口

- **Library API**: `from bus_foundation import publish, subscribe, schedule` 

## 2. 上游依赖



## 3. 下游影响

- agora
- runtime
- aetherforge (packages/gateway/)
- omo

## 4. 配置 / SSOT

- 项目源码：`projects/bus-foundation/`
- 入口定义：`projects/bus-foundation/pyproject.toml` 或 `package.json`
- 测试：`cd projects/bus-foundation && uv run pytest -q`

## 架构演进与项目边界索引

参见工作区架构演进与项目边界：[`../../docs/ARCHITECTURE-EVOLUTION.md`](../../docs/ARCHITECTURE-EVOLUTION.md)
