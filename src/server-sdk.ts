import express from 'express';
import cors from 'cors';
import { randomUUID } from 'crypto';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js';
import { KommoAPI } from './kommo-api.js';
import dotenv from 'dotenv';

dotenv.config();

const PORT = Number(process.env.PORT) || 3001;
const HOST = process.env.MCP_HOST || '0.0.0.0';

// Initialize Kommo API
const kommoAPI = new KommoAPI({
  baseUrl: process.env.KOMMO_BASE_URL || 'https://api-g.kommo.com',
  accessToken: process.env.KOMMO_ACCESS_TOKEN || ''
});

// Create MCP server
function createMcpServer(): Server {
  const server = new Server(
    {
      name: 'kommo-mcp-server',
      version: '2.0.0',
    },
    {
      capabilities: {
        tools: {},
      },
    }
  );

  // List available tools
  server.setRequestHandler(ListToolsRequestSchema, async () => {
    return {
      tools: [
        {
          name: 'get_leads',
          description: 'Listar leads do Kommo CRM',
          inputSchema: {
            type: 'object',
            properties: {
              limit: { type: 'number', description: 'Número máximo de leads (padrão: 50)' },
              page: { type: 'number', description: 'Página (padrão: 1)' }
            }
          }
        },
        {
          name: 'create_lead',
          description: 'Criar um novo lead no Kommo CRM',
          inputSchema: {
            type: 'object',
            properties: {
              name: { type: 'string', description: 'Nome do lead' },
              price: { type: 'number', description: 'Valor do lead' },
              status_id: { type: 'number', description: 'ID do status' }
            },
            required: ['name']
          }
        },
        {
          name: 'get_contacts',
          description: 'Listar contatos do Kommo CRM',
          inputSchema: {
            type: 'object',
            properties: {
              limit: { type: 'number', description: 'Número máximo (padrão: 50)' },
              page: { type: 'number', description: 'Página (padrão: 1)' }
            }
          }
        },
        {
          name: 'get_companies',
          description: 'Listar empresas do Kommo CRM',
          inputSchema: {
            type: 'object',
            properties: {
              limit: { type: 'number' },
              page: { type: 'number' }
            }
          }
        },
        {
          name: 'get_tasks',
          description: 'Listar tarefas do Kommo CRM',
          inputSchema: {
            type: 'object',
            properties: {
              limit: { type: 'number' },
              page: { type: 'number' }
            }
          }
        },
        {
          name: 'get_pipelines',
          description: 'Listar pipelines (funis) do Kommo CRM',
          inputSchema: {
            type: 'object',
            properties: {}
          }
        },
        {
          name: 'get_sales_report',
          description: 'Obter relatório de vendas em um período',
          inputSchema: {
            type: 'object',
            properties: {
              dateFrom: { type: 'string', description: 'Data inicial (YYYY-MM-DD)' },
              dateTo: { type: 'string', description: 'Data final (YYYY-MM-DD)' }
            }
          }
        }
      ]
    };
  });

  // Handle tool calls
  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;

    try {
      let data: any;

      switch (name) {
        case 'get_leads':
          data = await kommoAPI.getLeads({
            limit: (args?.limit as number) || 50,
            page: (args?.page as number) || 1
          });
          break;

        case 'create_lead':
          data = await kommoAPI.createLead(args);
          break;

        case 'get_contacts':
          data = await kommoAPI.getContacts({
            limit: (args?.limit as number) || 50,
            page: (args?.page as number) || 1
          });
          break;

        case 'get_companies':
          data = await kommoAPI.getCompanies({
            limit: (args?.limit as number) || 50,
            page: (args?.page as number) || 1
          });
          break;

        case 'get_tasks':
          data = await kommoAPI.getTasks({
            limit: (args?.limit as number) || 50,
            page: (args?.page as number) || 1
          });
          break;

        case 'get_pipelines':
          data = await kommoAPI.getPipelines();
          break;

        case 'get_sales_report':
          const dateFrom = (args?.dateFrom as string) || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
          const dateTo = (args?.dateTo as string) || new Date().toISOString().slice(0, 10);
          data = await kommoAPI.getSalesReport(dateFrom, dateTo);
          break;

        default:
          throw new Error(`Ferramenta desconhecida: ${name}`);
      }

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(data, null, 2)
          }
        ]
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return {
        content: [
          {
            type: 'text',
            text: `Erro ao executar ${name}: ${message}`
          }
        ],
        isError: true
      };
    }
  });

  return server;
}

// Express app setup
const app = express();
app.use(cors({
  origin: '*',
  exposedHeaders: ['Mcp-Session-Id'],
  allowedHeaders: ['Content-Type', 'mcp-session-id']
}));
app.use(express.json());

// Health endpoint
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    environment: process.env.NODE_ENV || 'production',
    kommo_base_url: process.env.KOMMO_BASE_URL
  });
});

// Session storage
const transports: { [sessionId: string]: StreamableHTTPServerTransport } = {};

// MCP endpoint - handles GET, POST, DELETE per Streamable HTTP spec
app.all('/mcp', async (req, res) => {
  try {
    const sessionId = req.headers['mcp-session-id'] as string | undefined;
    let transport: StreamableHTTPServerTransport;

    if (sessionId && transports[sessionId]) {
      // Existing session
      transport = transports[sessionId];
    } else if (!sessionId && req.method === 'POST' && req.body?.method === 'initialize') {
      // New initialization request
      transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => randomUUID(),
        onsessioninitialized: (newSessionId) => {
          transports[newSessionId] = transport;
          console.log(`[${new Date().toISOString()}] Session initialized: ${newSessionId}`);
        }
      });

      transport.onclose = () => {
        if (transport.sessionId) {
          delete transports[transport.sessionId];
          console.log(`[${new Date().toISOString()}] Session closed: ${transport.sessionId}`);
        }
      };

      const server = createMcpServer();
      await server.connect(transport);
    } else {
      res.status(400).json({
        jsonrpc: '2.0',
        error: {
          code: -32000,
          message: 'Bad Request: No valid session ID or initialization request'
        },
        id: null
      });
      return;
    }

    await transport.handleRequest(req, res, req.body);
  } catch (error) {
    console.error('[MCP Error]', error);
    if (!res.headersSent) {
      res.status(500).json({
        jsonrpc: '2.0',
        error: {
          code: -32603,
          message: 'Internal server error'
        },
        id: null
      });
    }
  }
});

// Start server
app.listen(PORT, HOST, () => {
  console.log(`[${new Date().toISOString()}] 🚀 Kommo MCP Server (SDK v2.0) running on http://${HOST}:${PORT}`);
  console.log(`[${new Date().toISOString()}] 📡 MCP endpoint: http://${HOST}:${PORT}/mcp`);
  console.log(`[${new Date().toISOString()}] 💚 Health: http://${HOST}:${PORT}/health`);
  console.log(`[${new Date().toISOString()}] 🔗 Kommo: ${process.env.KOMMO_BASE_URL}`);
});
