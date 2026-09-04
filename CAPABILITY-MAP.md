---
type: ssot
owner: governance-team
last_updated: 2026-09-03
---

# bus-foundation 能力地图

> Omni-Bus 三平面 (Data/Event/Control) 统一接口 · X 层横切

---

## 一、架构定位

```
┌─────────────────────────────────────────────────────────────┐
│                bus-foundation — Omni-Bus 总线基座               │
├─────────────────────────────────────────────────────────────┤
│  Data 平面  │  Event 平面  │  Control 平面  │  DLQ 容错     │
│  Envelope   │  Backends    │  Router         │  Dead Letter  │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心功能

| 功能 | 说明 |
|------|------|
| 统一信封 | Omni-Bus Envelope 标准消息封装 |
| 多平面路由 | Data / Event / Control 三平面分发 |
| 后端抽象 | asyncio / croniter / messagebus / eventbus 多后端 |
| 死信队列 | SQLite DLQ 容错, 失败消息可重放 |

---

## 三、模块结构

| 模块 | 职责 |
|------|------|
| `envelope.py` | Omni-Bus 统一信封实现 |
| `router.py` | 分发到后端, 失败回退 DLQ |
| `dlq.py` | SQLite 死信队列 |
| `__init__.py` | 统一总线接口 (facade) |

---

## 四、依赖方向

- **上游**: 被 `agora` (BOS URI 路由) 和 `runtime` (事件分发) 依赖
- **下游**: 不反向依赖 `agora` (保持通用包独立性)
- **BOS 域**: 见 `docs/project-registry.yaml`

---

*项目元数据: [`../../docs/project-registry.yaml`](../../docs/project-registry.yaml)*
