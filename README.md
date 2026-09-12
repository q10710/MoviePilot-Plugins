<div align="center">

# MoviePilot-Plugins

**MoviePilot V2 / V3 自用插件仓库**

[![MoviePilot](https://img.shields.io/badge/MoviePilot-V2%20%7C%20V3-1f6feb?style=flat-square&logo=appveyor&logoColor=white)](https://github.com/jxxghp/MoviePilot)
[![Plugins](https://img.shields.io/badge/插件-10%20个-3fb950?style=flat-square)](#插件清单)
[![Author](https://img.shields.io/badge/作者-Q-8957e5?style=flat-square)](https://github.com/q10710)
[![Last Commit](https://img.shields.io/github/last-commit/q10710/MoviePilot-Plugins?style=flat-square&color=8b949e)](https://github.com/q10710/MoviePilot-Plugins/commits/main)

</div>

---

自己写、自己改、自己用。用得顺手的东西，就整理出来分享。

所有改造版都保留了明确的上游来源说明，方便对照上游更新。

## 插件清单

| ｜ | 插件 | 说明 | 上游来源 |
| :--: | :--- | :--- | :--- |
| 📡 | **订阅助手Q自用版** | 订阅全生命周期管理。H&R 种子下载超时不直接删种，改为移入收容目录保种，按站点 H&R 时长到期后再清理 | 官方 SubscribeAssistantEnhanced |
| 🧲 | **H&R助手Q自用版** | H&R 种子标签与做种时长跟踪，支持多下载器，可识别种子在下载器之间的转移做种 | 官方 HitAndRun |
| 🌊 | **站点流量管理Q自用版** | 按站点分享率自动管理刷流任务：低于下限按最终配置新建并启动，高于上限只暂停不删除 | 官方 TrafficAssistant |
| ⚡ | **自动限速Q自用版** | 按标签或全局给 qb / tr 的下载任务限速 | 第三方「自动限速」 |
| 🛡️ | **做种守卫Q自用版** | 检测无效做种（文件丢失、tracker 全部失败）与孤儿源文件，连续达标后按策略处置 | 自建 |
| 🔗 | **硬链接检查Q自用版** | 清理「曾经被硬链接、如今链接断开」的残留文件；并清理媒体库里只剩元数据的空壳季目录 | 自建 |
| 🎬 | **洗版守护Q自用版** | 未完结却被误标洗版的订阅自动取消洗版；媒体库文件丢失时自动重置订阅重新下载 | 自建 |
| 🧹 | **过期订阅清理Q自用版** | 订阅超过设定天数未下载到新剧集时自动取消 | 自建 |
| 🗑️ | **删档订阅清理Q自用版** | 媒体库条目被删除而订阅仍在、长期不下载时，按配置清理该订阅 | 自建 |

> 插件显示名统一为「原名 + Q自用版」，作者统一为 `Q`，**插件 ID 保持不变**，因此升级不会影响已有配置与数据。

## 安装

在 MoviePilot 的 `PLUGIN_MARKET` 中加入本仓库地址：

```text
https://github.com/q10710/MoviePilot-Plugins
```

也可以在插件页面点击图标维护插件库地址，或在插件市场设置里点「同步 Wiki」。

刷新插件市场，搜索插件名（例如「硬链接检查」），安装即可。

> 插件市场只读取仓库 `main` 分支，需要能正常访问 GitHub。建议配置好 `GITHUB_TOKEN`，避免触发 API 限流。

## 更新

插件页会显示可更新版本，也可以直接点更新。

每个插件的更新说明写在 `package.v2.json` 的 `history` 里，升级前可先看变更内容。

## 注意事项

- 本仓库插件以自用为主，配置项会随实际使用调整，升级前建议留意更新说明。
- 涉及删除文件、删除订阅、修改下载器的插件（硬链接检查、做种守卫、删档订阅清理等），**相关开关默认关闭或只报告不删除**，请确认判据符合你的目录结构后再开启。
- 使用中遇到问题，请附上插件日志（`/config/logs/plugins/<插件名小写>.log`）提 Issue。

## 许可

本仓库代码仅用于个人学习与自用。改造版代码版权归上游原作者所有，请遵循上游项目的许可协议。
