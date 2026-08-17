# astrbot_plugin_group_context_flow

将群聊消息持久化为连续 flow，并在 AstrBot 调用 LLM 前按 conversation 增量注入。

## 行为

- 只处理群聊消息。
- 插件记录群内消息到 `data/plugin_data/astrbot_plugin_group_context_flow/`（追加式 JSONL 日志，仅当记录数超上限时才整文件裁剪一次）。
- 触发 LLM 时，插件把当前触发消息之前、尚未注入当前 `conversation.cid` 的群聊增量作为**临时内容块**合并进当前用户消息（`extra_user_content_parts`），通过 `mark_as_temp()` 标记为**不写入 `conversation.history`**，从而避免群聊上下文在历史中累积膨胀。
- 当前触发 AI 的消息不放进增量上下文，仍保留为 AstrBot 本轮 user prompt；响应成功后 cursor 会推进到当前触发消息，避免下一轮重复出现。
- 增量可通过 `max_delta_messages`（条数）和 `max_delta_seconds`（时间窗口，秒）双重裁剪，同时满足两者才注入；任一项为 `0` 表示该项不限制。
- 当条数裁剪产生被丢弃的旧消息时，若开启 `summarize_overflow`，会用 LLM 把溢出部分压缩成 `<group_summary>` 结构化摘要块注入，避免简单丢弃旧上下文。
- 如果平台把 LLM 回复回流成机器人自己的群消息，插件会无条件跳过；`record_self_messages` 只影响非 LLM 流程产生的机器人自身平台消息。
- 内置 `/reset` 或 `/new` 成功后，插件会把当前 conversation 的增量边界推进到指令消息本身；原始群聊 flow 不会被清空，但新/重置后的对话不会回灌旧消息。
- 增量默认不进历史；如需模型跨轮引用群聊内容，可开启 `persist_delta_in_history`（代价是历史膨胀，不推荐）。
- AstrBot 后续仍会对 `conversation.history` 使用自身的上下文截断或 LLM 压缩策略。

## Request Message 形态

注入后，模型收到的当前用户消息类似：

```json
[
  {
    "role": "system",
    "content": "# Persona Instructions\n..."
  },
  {
    "role": "user",
    "content": [
      {"type": "text", "text": "@Bot 那就定寿司吗？"},
      {"type": "text", "text": "<group_messages_delta>\n[Bob/20:05:02]: 我不吃辣\n---\n[Carol/20:05:20]: 那寿司更稳\n</group_messages_delta>"}
    ]
  }
]
```

开启 `summarize_overflow` 且产生溢出时，会额外注入：

```json
{"type": "text", "text": "<group_summary>\n## 已确认的事实/决定\n...\n</group_summary>"}
```

## 建议配置

- 建议关闭 AstrBot 内置的群聊上下文感知/聊天记忆增强，避免同一段群聊历史被重复注入。
- 如果担心首次启用时已有群聊日志过长，可设置 `max_delta_messages` 和/或 `max_delta_seconds`。默认 `0` 表示不限制，由 AstrBot 的上下文上限处理逻辑接管。
- 群聊活跃度高时，建议设置时间窗口（如 `600` 秒）来限制单次注入的消息范围。

## 指令

- `/gflow_status`：查看当前群聊 flow 记录数、最新 seq 和当前 conversation cursor。
- `/gflow_clear`：管理员清空当前群聊的插件原始 flow 日志和 cursor。
