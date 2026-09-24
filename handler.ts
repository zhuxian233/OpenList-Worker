import { handle } from "hono/aws-lambda"
import app from "./src/backend/index"

// 挂载 MCP 路由到现有的 Hono 实例
app.all("/mcp", (c) => handleMcpRequest(c.req.raw, c.env, app))
app.all("/mcp/*", (c) => handleMcpRequest(c.req.raw, c.env, app))

export const handler = handle(app)

// ==================== 自定义 MCP 服务适配层 ====================
async function handleMcpRequest(request: Request, env: any, honoApp: typeof app): Promise<Response> {
  const url = new URL(request.url);

  // 1. 处理跨域预检请求 (CORS)
  if (request.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
      },
    });
  }

  // 2. 处理 SSE 建立连接 (GET /mcp)
  if (request.method === "GET") {
    const sessionId = crypto.randomUUID();
    const endpointUrl = `${url.origin}/mcp/message?sessionId=${sessionId}`;

    const stream = new ReadableStream({
      start(controller) {
        const encoder = new TextEncoder();
        controller.enqueue(encoder.encode(`event: endpoint\ndata: ${endpointUrl}\n\n`));
      },
    });

    return new Response(stream, {
      headers: {
        "Content-Type": "text/event-stream; charset=utf-8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Access-Control-Allow-Origin": "*",
      },
    });
  }

  // 3. 处理 JSON-RPC 指令 (POST /mcp 或 POST /mcp/message)
  if (request.method === "POST") {
    let body: any;
    try {
      body = await request.json();
    } catch {
      return new Response(JSON.stringify({ error: "Invalid JSON" }), { status: 400 });
    }

    const { id, method, params } = body;
    let result: any = null;

    if (method === "initialize") {
      result = {
        protocolVersion: "2024-11-05",
        capabilities: { tools: {} },
        serverInfo: { name: "openlist-mcp-server", version: "1.0.0" },
      };
    } else if (method === "notifications/initialized") {
      return new Response(null, {
        status: 204,
        headers: { "Access-Control-Allow-Origin": "*" },
      });
    } else if (method === "tools/list") {
      result = {
        tools: [
          {
            name: "list_files",
            description: "列出指定网盘路径下的所有文件和子目录列表",
            inputSchema: {
              type: "object",
              properties: {
                path: {
                  type: "string",
                  description: "目录路径，根目录传 '/'，例如 '/百度网盘'",
                },
              },
              required: ["path"],
            },
          },
          {
            name: "get_file_info",
            description: "获取特定网盘文件的详情、大小以及直接下载直链",
            inputSchema: {
              type: "object",
              properties: {
                path: {
                  type: "string",
                  description: "文件的完整路径，例如 '/百度网盘/资料.pdf'",
                },
              },
              required: ["path"],
            },
          },
        ],
      };
    } else if (method === "tools/call") {
      const toolName = params?.name;
      const args = params?.arguments || {};

      try {
        if (toolName === "list_files") {
          // 使用 honoApp.request 替代外部网络 fetch，速度更快且无需经过网关
          const listRes = await honoApp.request("/api/fs/list", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ path: args.path || "/", page: 1, per_page: 0 }),
          });
          const data: any = await listRes.json();
          result = {
            content: [{ type: "text", text: JSON.stringify(data?.data?.content || data) }],
          };
        } else if (toolName === "get_file_info") {
          const getRes = await honoApp.request("/api/fs/get", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ path: args.path }),
          });
          const data: any = await getRes.json();
          result = {
            content: [{ type: "text", text: JSON.stringify(data?.data || data) }],
          };
        }
      } catch (e: any) {
        result = {
          content: [{ type: "text", text: `执行失败: ${e.message}` }],
          isError: true,
        };
      }
    }

    return new Response(JSON.stringify({ jsonrpc: "2.0", id, result }), {
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
    });
  }

  return new Response("Method not allowed", { status: 405 });
}

export const handler = handle(app)
// ==================== 自定义 MCP 服务适配层 ====================
async function handleMcpRequest(request: Request, env: any): Promise<Response> {
  const url = new URL(request.url);

  // 1. 处理跨域预检请求 (CORS)
  if (request.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
      },
    });
  }

  // 2. 处理 Spark 发起的 SSE 建立连接 (GET /mcp)
  if (request.method === "GET") {
    const sessionId = crypto.randomUUID();
    const endpointUrl = `${url.origin}/mcp/message?sessionId=${sessionId}`;

    const stream = new ReadableStream({
      start(controller) {
        const encoder = new TextEncoder();
        // 按照 MCP SSE 标准协议推送消息上报端点
        controller.enqueue(encoder.encode(`event: endpoint\ndata: ${endpointUrl}\n\n`));
      },
    });

    return new Response(stream, {
      headers: {
        "Content-Type": "text/event-stream; charset=utf-8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Access-Control-Allow-Origin": "*",
      },
    });
  }

  // 3. 处理 Spark 发来的 JSON-RPC 交互指令 (POST /mcp 或 POST /mcp/message)
  if (request.method === "POST") {
    let body: any;
    try {
      body = await request.json();
    } catch {
      return new Response(JSON.stringify({ error: "Invalid JSON" }), { status: 400 });
    }

    const { id, method, params } = body;
    let result: any = null;

    // 握手阶段：初始化协议与支持的功能
    if (method === "initialize") {
      result = {
        protocolVersion: "2024-11-05",
        capabilities: { tools: {} },
        serverInfo: { name: "openlist-mcp-server", version: "1.0.0" },
      };
    } else if (method === "notifications/initialized") {
      return new Response(null, {
        status: 204,
        headers: { "Access-Control-Allow-Origin": "*" },
      });
    }
    // 工具列表阶段：向 Spark 宣告可用技能
    else if (method === "tools/list") {
      result = {
        tools: [
          {
            name: "list_files",
            description: "列出指定网盘路径下的所有文件和子目录列表",
            inputSchema: {
              type: "object",
              properties: {
                path: {
                  type: "string",
                  description: "目录路径，根目录传 '/'，例如 '/百度网盘'",
                },
              },
              required: ["path"],
            },
          },
          {
            name: "get_file_info",
            description: "获取特定网盘文件的详情、大小以及直接下载直链",
            inputSchema: {
              type: "object",
              properties: {
                path: {
                  type: "string",
                  description: "文件的完整路径，例如 '/百度网盘/高考复习资料.pdf'",
                },
              },
              required: ["path"],
            },
          },
        ],
      };
    }
    // 工具执行阶段：AI 下达指令时调用 OpenList 本地接口
    else if (method === "tools/call") {
      const toolName = params?.name;
      const args = params?.arguments || {};

      try {
        if (toolName === "list_files") {
          const listRes = await fetch(`${url.origin}/api/fs/list`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ path: args.path || "/", page: 1, per_page: 0 }),
          });
          const data: any = await listRes.json();
          result = {
            content: [{ type: "text", text: JSON.stringify(data?.data?.content || data) }],
          };
        } else if (toolName === "get_file_info") {
          const getRes = await fetch(`${url.origin}/api/fs/get`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ path: args.path }),
          });
          const data: any = await getRes.json();
          result = {
            content: [{ type: "text", text: JSON.stringify(data?.data || data) }],
          };
        }
      } catch (e: any) {
        result = {
          content: [{ type: "text", text: `执行失败: ${e.message}` }],
          isError: true,
        };
      }
    }

    return new Response(JSON.stringify({ jsonrpc: "2.0", id, result }), {
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
    });
  }

  return new Response("Method not allowed", { status: 405 });
}
