import express from 'express';
import cors from 'cors';
import { randomUUID } from 'crypto';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js';
import { KommoAPI, KommoStatus, KommoPipeline, KommoUser } from './kommo-api.js';
import dotenv from 'dotenv';

dotenv.config();

const PORT = Number(process.env.PORT) || 3001;
const HOST = process.env.MCP_HOST || '0.0.0.0';

const kommoAPI = new KommoAPI({
  baseUrl: process.env.KOMMO_BASE_URL || 'https://api-g.kommo.com',
  accessToken: process.env.KOMMO_ACCESS_TOKEN || ''
});

// =====================================================================
// CACHE de pipelines, statuses e usuários (refresh a cada 10 min)
// =====================================================================
interface MetadataCache {
  pipelines: Map<number, KommoPipeline>;
  statuses: Map<number, KommoStatus>;
  users: Map<number, KommoUser>;
  lastRefresh: number;
}

const cache: MetadataCache = {
  pipelines: new Map(),
  statuses: new Map(),
  users: new Map(),
  lastRefresh: 0,
};

const CACHE_TTL = 10 * 60 * 1000;

async function refreshCache(force = false): Promise<void> {
  if (!force && Date.now() - cache.lastRefresh < CACHE_TTL && cache.pipelines.size > 0) return;

  try {
    const [pipelinesRes, usersRes] = await Promise.all([
      kommoAPI.getPipelines().catch(() => null),
      kommoAPI.getUsers().catch(() => null),
    ]);

    cache.pipelines.clear();
    cache.statuses.clear();
    cache.users.clear();

    const pipelines: KommoPipeline[] = pipelinesRes?._embedded?.pipelines || [];
    for (const p of pipelines) {
      cache.pipelines.set(p.id, p);
      const statuses: KommoStatus[] = p._embedded?.statuses || [];
      for (const s of statuses) cache.statuses.set(s.id, s);
    }

    const users: KommoUser[] = usersRes?._embedded?.users || [];
    for (const u of users) cache.users.set(u.id, u);

    cache.lastRefresh = Date.now();
    console.log(`[cache] refreshed: ${cache.pipelines.size} pipelines, ${cache.statuses.size} statuses, ${cache.users.size} users`);
  } catch (err) {
    console.error('[cache] refresh failed:', err);
  }
}

function enrichLead(lead: any): any {
  if (!lead) return lead;
  const status = cache.statuses.get(lead.status_id);
  const pipeline = cache.pipelines.get(lead.pipeline_id);
  const user = cache.users.get(lead.responsible_user_id);

  return {
    ...lead,
    status_name: status?.name || null,
    pipeline_name: pipeline?.name || null,
    responsible_user_name: user?.name || null,
    is_won: status?.type === 1,
    is_lost: status?.type === 2,
  };
}

// =====================================================================
// Helpers para construir filtros
// =====================================================================
function buildLeadsParams(args: any): any {
  const params: any = {};

  if (args.limit) params.limit = Math.min(args.limit, 250);
  if (args.page) params.page = args.page;
  if (args.query) params.query = args.query;
  if (args.order_by) {
    const dir = args.order_dir === 'desc' ? 'desc' : 'asc';
    params[`order[${args.order_by}]`] = dir;
  }
  if (args.with) params.with = args.with;

  // filter[pipeline_id]
  if (args.pipeline_id) params['filter[pipeline_id]'] = args.pipeline_id;

  // filter[responsible_user_id]
  if (args.responsible_user_id) params['filter[responsible_user_id]'] = args.responsible_user_id;

  // filter[statuses][N][pipeline_id] / [status_id]
  if (Array.isArray(args.statuses)) {
    args.statuses.forEach((st: any, i: number) => {
      if (st.pipeline_id) params[`filter[statuses][${i}][pipeline_id]`] = st.pipeline_id;
      if (st.status_id) params[`filter[statuses][${i}][status_id]`] = st.status_id;
    });
  }

  // filter[created_at][from / to]
  if (args.created_from) params['filter[created_at][from]'] = args.created_from;
  if (args.created_to) params['filter[created_at][to]'] = args.created_to;

  // filter[updated_at][from / to]
  if (args.updated_from) params['filter[updated_at][from]'] = args.updated_from;
  if (args.updated_to) params['filter[updated_at][to]'] = args.updated_to;

  // filter[closed_at][from / to]
  if (args.closed_from) params['filter[closed_at][from]'] = args.closed_from;
  if (args.closed_to) params['filter[closed_at][to]'] = args.closed_to;

  return params;
}

// Converte data ISO/string para timestamp Unix se necessário
function toUnixTimestamp(input: any): number | undefined {
  if (input == null) return undefined;
  if (typeof input === 'number') return input > 1e12 ? Math.floor(input / 1000) : input;
  const d = new Date(input);
  if (isNaN(d.getTime())) return undefined;
  return Math.floor(d.getTime() / 1000);
}

// =====================================================================
// MCP Server
// =====================================================================
function createMcpServer(): Server {
  const server = new Server(
    { name: 'kommo-mcp-server', version: '3.0.0' },
    { capabilities: { tools: {} } }
  );

  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: [
      // ========== LEITURA ==========
      {
        name: 'get_leads',
        description: 'Lista leads do Kommo com filtros completos. Aceita filtros por pipeline, status, vendedor responsável e intervalos de datas (created_at, updated_at, closed_at). Retornos vêm enriquecidos com status_name, pipeline_name, responsible_user_name, is_won e is_lost. Use closed_from/closed_to para responder "vendas no período X". Datas aceitam formato ISO (YYYY-MM-DD) ou timestamp Unix.',
        inputSchema: {
          type: 'object',
          properties: {
            limit: { type: 'number', description: 'Máximo por página (até 250, padrão 50)' },
            page: { type: 'number', description: 'Página (padrão 1)' },
            query: { type: 'string', description: 'Busca textual livre' },
            pipeline_id: { type: 'number', description: 'Filtrar por funil' },
            responsible_user_id: { type: 'number', description: 'Filtrar por vendedor responsável' },
            statuses: {
              type: 'array',
              description: 'Filtrar por status. Array de {pipeline_id, status_id}',
              items: {
                type: 'object',
                properties: {
                  pipeline_id: { type: 'number' },
                  status_id: { type: 'number' }
                }
              }
            },
            created_from: { type: 'string', description: 'Criados a partir de (ISO YYYY-MM-DD ou Unix)' },
            created_to: { type: 'string', description: 'Criados até (ISO YYYY-MM-DD ou Unix)' },
            updated_from: { type: 'string', description: 'Atualizados a partir de' },
            updated_to: { type: 'string', description: 'Atualizados até' },
            closed_from: { type: 'string', description: 'Fechados a partir de (ISO ou Unix). Use para "vendas/perdas no período"' },
            closed_to: { type: 'string', description: 'Fechados até' },
            order_by: { type: 'string', enum: ['created_at', 'updated_at', 'id'], description: 'Campo de ordenação' },
            order_dir: { type: 'string', enum: ['asc', 'desc'] },
            with: { type: 'string', description: 'Campos extras: contacts,catalog_elements,loss_reason,is_price_modified_by_robot' }
          }
        }
      },
      {
        name: 'get_lead_by_id',
        description: 'Detalhe completo de um lead, incluindo notas, tags, contatos vinculados e custom fields. Retorno enriquecido com nomes legíveis.',
        inputSchema: {
          type: 'object',
          properties: {
            id: { type: 'number', description: 'ID do lead' },
            with: { type: 'string', description: 'Campos extras (ex: contacts,loss_reason,catalog_elements)' }
          },
          required: ['id']
        }
      },
      {
        name: 'get_lead_notes',
        description: 'Lista as notas (anotações) de um lead. Onde fica a história da negociação.',
        inputSchema: {
          type: 'object',
          properties: {
            lead_id: { type: 'number' },
            limit: { type: 'number' },
            page: { type: 'number' }
          },
          required: ['lead_id']
        }
      },
      {
        name: 'get_lead_events',
        description: 'Histórico de eventos de um lead específico (mudanças de status, criação, edições). Para reconstruir a linha do tempo.',
        inputSchema: {
          type: 'object',
          properties: {
            lead_id: { type: 'number' },
            limit: { type: 'number' }
          },
          required: ['lead_id']
        }
      },
      {
        name: 'get_events',
        description: 'Eventos do CRM com filtros. Endpoint correto para reconstruir histórico de fechamentos: filter[type]=lead_status_changed combinado com value_after.leads_statuses apontando para o status "Ganho" do funil. Suporta filter[created_at][from/to].',
        inputSchema: {
          type: 'object',
          properties: {
            type: { type: 'string', description: 'Tipo do evento (ex: lead_status_changed, lead_added)' },
            entity_type: { type: 'string', enum: ['lead', 'contact', 'company'] },
            entity_id: { type: 'number' },
            created_from: { type: 'string', description: 'ISO ou Unix' },
            created_to: { type: 'string' },
            limit: { type: 'number' },
            page: { type: 'number' }
          }
        }
      },
      {
        name: 'get_pipelines',
        description: 'Lista todos os funis com seus status (etapas). Status com type=1 são "Ganho" e type=2 são "Perdido".',
        inputSchema: { type: 'object', properties: {} }
      },
      {
        name: 'get_pipeline_statuses',
        description: 'Lista os status (etapas) de um funil específico.',
        inputSchema: {
          type: 'object',
          properties: { pipeline_id: { type: 'number' } },
          required: ['pipeline_id']
        }
      },
      {
        name: 'get_users',
        description: 'Lista todos os usuários (vendedores) da conta.',
        inputSchema: { type: 'object', properties: {} }
      },
      {
        name: 'get_loss_reasons',
        description: 'Lista os motivos de perda configurados na conta.',
        inputSchema: { type: 'object', properties: {} }
      },
      {
        name: 'get_contacts',
        description: 'Lista contatos do Kommo CRM.',
        inputSchema: {
          type: 'object',
          properties: {
            limit: { type: 'number' },
            page: { type: 'number' },
            query: { type: 'string' }
          }
        }
      },
      {
        name: 'get_companies',
        description: 'Lista empresas do Kommo CRM.',
        inputSchema: {
          type: 'object',
          properties: {
            limit: { type: 'number' },
            page: { type: 'number' },
            query: { type: 'string' }
          }
        }
      },
      {
        name: 'get_tasks',
        description: 'Lista tarefas do Kommo CRM.',
        inputSchema: {
          type: 'object',
          properties: {
            limit: { type: 'number' },
            page: { type: 'number' },
            responsible_user_id: { type: 'number' },
            entity_type: { type: 'string', enum: ['leads', 'contacts', 'companies'] },
            is_completed: { type: 'boolean' }
          }
        }
      },
      {
        name: 'get_lead_custom_fields',
        description: 'Definições dos custom fields configurados para leads.',
        inputSchema: { type: 'object', properties: {} }
      },

      // ========== ESCRITA ==========
      {
        name: 'create_lead',
        description: 'Cria um novo lead no Kommo CRM.',
        inputSchema: {
          type: 'object',
          properties: {
            name: { type: 'string' },
            price: { type: 'number' },
            status_id: { type: 'number' },
            pipeline_id: { type: 'number' },
            responsible_user_id: { type: 'number' },
            tags: { type: 'array', items: { type: 'string' } }
          },
          required: ['name']
        }
      },
      {
        name: 'update_lead',
        description: 'Atualiza um lead existente. Pode mudar nome, valor, status, pipeline, responsável.',
        inputSchema: {
          type: 'object',
          properties: {
            id: { type: 'number' },
            name: { type: 'string' },
            price: { type: 'number' },
            status_id: { type: 'number' },
            pipeline_id: { type: 'number' },
            responsible_user_id: { type: 'number' }
          },
          required: ['id']
        }
      },
      {
        name: 'move_lead_status',
        description: 'Move um lead para outro status do funil. Atalho semântico para update_lead.',
        inputSchema: {
          type: 'object',
          properties: {
            lead_id: { type: 'number' },
            status_id: { type: 'number' },
            pipeline_id: { type: 'number', description: 'Opcional, se mover entre funis' }
          },
          required: ['lead_id', 'status_id']
        }
      },
      {
        name: 'add_note_to_lead',
        description: 'Adiciona uma nota (anotação) a um lead. Útil para registrar resumos de conversa.',
        inputSchema: {
          type: 'object',
          properties: {
            lead_id: { type: 'number' },
            text: { type: 'string' }
          },
          required: ['lead_id', 'text']
        }
      },
      {
        name: 'create_task',
        description: 'Cria uma tarefa associada a um lead, contato ou empresa.',
        inputSchema: {
          type: 'object',
          properties: {
            text: { type: 'string', description: 'Texto da tarefa' },
            entity_id: { type: 'number' },
            entity_type: { type: 'string', enum: ['leads', 'contacts', 'companies'] },
            complete_till: { type: 'number', description: 'Timestamp Unix do prazo' },
            responsible_user_id: { type: 'number' }
          },
          required: ['text', 'entity_id', 'entity_type', 'complete_till']
        }
      },
      {
        name: 'complete_task',
        description: 'Marca uma tarefa como concluída.',
        inputSchema: {
          type: 'object',
          properties: {
            task_id: { type: 'number' },
            result_text: { type: 'string', description: 'Resultado/comentário da tarefa' }
          },
          required: ['task_id']
        }
      }
    ]
  }));

  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;
    const a: any = args || {};

    try {
      // Garante cache pronto antes de processar
      await refreshCache();

      let payload: any;

      switch (name) {
        // ========== LEITURA ==========
        case 'get_leads': {
          // Converte datas ISO para Unix se vierem como string
          const filterArgs = { ...a };
          ['created_from', 'created_to', 'updated_from', 'updated_to', 'closed_from', 'closed_to'].forEach(k => {
            if (filterArgs[k] != null) {
              const ts = toUnixTimestamp(filterArgs[k]);
              if (ts !== undefined) filterArgs[k] = ts;
            }
          });
          if (!filterArgs.limit) filterArgs.limit = 50;

          const params = buildLeadsParams(filterArgs);
          const data = await kommoAPI.getLeads(params);
          const leads = (data._embedded?.leads || []).map(enrichLead);

          payload = {
            count: leads.length,
            page: data._page || filterArgs.page || 1,
            has_next: !!data._links?.next,
            leads,
          };
          break;
        }

        case 'get_lead_by_id': {
          const lead = await kommoAPI.getLead(a.id, a.with);
          payload = enrichLead(lead);
          break;
        }

        case 'get_lead_notes': {
          const params: any = {};
          if (a.limit) params.limit = a.limit;
          if (a.page) params.page = a.page;
          payload = await kommoAPI.getLeadNotes(a.lead_id, params);
          break;
        }

        case 'get_lead_events': {
          const params: any = {};
          if (a.limit) params.limit = a.limit;
          payload = await kommoAPI.getLeadEvents(a.lead_id, params);
          break;
        }

        case 'get_events': {
          const params: any = {};
          if (a.type) params['filter[type]'] = a.type;
          if (a.entity_type) params['filter[entity]'] = a.entity_type;
          if (a.entity_id) params['filter[entity_id]'] = a.entity_id;
          const cf = toUnixTimestamp(a.created_from);
          const ct = toUnixTimestamp(a.created_to);
          if (cf) params['filter[created_at][from]'] = cf;
          if (ct) params['filter[created_at][to]'] = ct;
          if (a.limit) params.limit = a.limit;
          if (a.page) params.page = a.page;
          payload = await kommoAPI.getEvents(params);
          break;
        }

        case 'get_pipelines': {
          await refreshCache(true); // força refresh nesse comando
          const pipelines = Array.from(cache.pipelines.values()).map(p => ({
            id: p.id,
            name: p.name,
            is_main: p.is_main,
            is_archive: p.is_archive,
            statuses: (p._embedded?.statuses || []).map(s => ({
              id: s.id,
              name: s.name,
              type: s.type,
              is_won: s.type === 1,
              is_lost: s.type === 2,
              color: s.color,
              sort: s.sort,
            }))
          }));
          payload = { count: pipelines.length, pipelines };
          break;
        }

        case 'get_pipeline_statuses': {
          payload = await kommoAPI.getPipelineStatuses(a.pipeline_id);
          break;
        }

        case 'get_users': {
          const data = await kommoAPI.getUsers();
          payload = data;
          break;
        }

        case 'get_loss_reasons': {
          payload = await kommoAPI.getLossReasons();
          break;
        }

        case 'get_contacts': {
          const params: any = { limit: a.limit || 50, page: a.page || 1 };
          if (a.query) params.query = a.query;
          payload = await kommoAPI.getContacts(params);
          break;
        }

        case 'get_companies': {
          const params: any = { limit: a.limit || 50, page: a.page || 1 };
          if (a.query) params.query = a.query;
          payload = await kommoAPI.getCompanies(params);
          break;
        }

        case 'get_tasks': {
          const params: any = { limit: a.limit || 50, page: a.page || 1 };
          if (a.responsible_user_id) params['filter[responsible_user_id]'] = a.responsible_user_id;
          if (a.entity_type) params['filter[entity_type]'] = a.entity_type;
          if (a.is_completed != null) params['filter[is_completed]'] = a.is_completed ? 1 : 0;
          payload = await kommoAPI.getTasks(params);
          break;
        }

        case 'get_lead_custom_fields': {
          payload = await kommoAPI.getLeadsCustomFields();
          break;
        }

        // ========== ESCRITA ==========
        case 'create_lead': {
          const leadPayload: any = { name: a.name };
          if (a.price != null) leadPayload.price = a.price;
          if (a.status_id) leadPayload.status_id = a.status_id;
          if (a.pipeline_id) leadPayload.pipeline_id = a.pipeline_id;
          if (a.responsible_user_id) leadPayload.responsible_user_id = a.responsible_user_id;
          if (Array.isArray(a.tags)) leadPayload._embedded = { tags: a.tags.map((t: string) => ({ name: t })) };
          payload = enrichLead(await kommoAPI.createLead(leadPayload));
          break;
        }

        case 'update_lead': {
          const update: any = {};
          ['name', 'price', 'status_id', 'pipeline_id', 'responsible_user_id'].forEach(k => {
            if (a[k] != null) update[k] = a[k];
          });
          payload = enrichLead(await kommoAPI.updateLead(a.id, update));
          break;
        }

        case 'move_lead_status': {
          const update: any = { status_id: a.status_id };
          if (a.pipeline_id) update.pipeline_id = a.pipeline_id;
          payload = enrichLead(await kommoAPI.updateLead(a.lead_id, update));
          break;
        }

        case 'add_note_to_lead': {
          payload = await kommoAPI.addNoteToLead(a.lead_id, a.text);
          break;
        }

        case 'create_task': {
          const taskPayload: any = {
            text: a.text,
            entity_id: a.entity_id,
            entity_type: a.entity_type,
            complete_till: a.complete_till,
          };
          if (a.responsible_user_id) taskPayload.responsible_user_id = a.responsible_user_id;
          payload = await kommoAPI.createTask(taskPayload);
          break;
        }

        case 'complete_task': {
          payload = await kommoAPI.completeTask(a.task_id, a.result_text);
          break;
        }

        default:
          throw new Error(`Ferramenta desconhecida: ${name}`);
      }

      return {
        content: [{ type: 'text', text: JSON.stringify(payload, null, 2) }]
      };

    } catch (err) {
      const e = KommoAPI.formatError(err);
      const errorMsg = {
        tool: name,
        error: {
          status: e.status,
          message: e.message,
          detail: e.detail,
          hint: e.status === 401 ? 'Token expirado ou inválido — verifique KOMMO_ACCESS_TOKEN' :
                e.status === 403 ? 'Sem permissão para essa operação' :
                e.status === 404 ? 'Recurso não encontrado — verifique IDs' :
                e.status === 429 ? 'Rate limit (7 req/s). Reduza concorrência ou aumente delay' :
                undefined
        }
      };
      return {
        content: [{ type: 'text', text: JSON.stringify(errorMsg, null, 2) }],
        isError: true,
      };
    }
  });

  return server;
}

// =====================================================================
// Express + transport
// =====================================================================
const app = express();
app.use(cors({
  origin: '*',
  exposedHeaders: ['Mcp-Session-Id'],
  allowedHeaders: ['Content-Type', 'mcp-session-id']
}));
app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({
    status: 'healthy',
    version: '3.0.0',
    timestamp: new Date().toISOString(),
    kommo_base_url: process.env.KOMMO_BASE_URL,
    cache: {
      pipelines: cache.pipelines.size,
      statuses: cache.statuses.size,
      users: cache.users.size,
      last_refresh: cache.lastRefresh ? new Date(cache.lastRefresh).toISOString() : null,
    }
  });
});

const transports: { [sessionId: string]: StreamableHTTPServerTransport } = {};

app.all('/mcp', async (req, res) => {
  try {
    const sessionId = req.headers['mcp-session-id'] as string | undefined;
    let transport: StreamableHTTPServerTransport;

    if (sessionId && transports[sessionId]) {
      transport = transports[sessionId];
    } else if (!sessionId && req.method === 'POST' && req.body?.method === 'initialize') {
      transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => randomUUID(),
        onsessioninitialized: (newSessionId) => {
          transports[newSessionId] = transport;
          console.log(`[mcp] session initialized: ${newSessionId}`);
        }
      });

      transport.onclose = () => {
        if (transport.sessionId) {
          delete transports[transport.sessionId];
          console.log(`[mcp] session closed: ${transport.sessionId}`);
        }
      };

      const server = createMcpServer();
      await server.connect(transport);
    } else {
      res.status(400).json({
        jsonrpc: '2.0',
        error: { code: -32000, message: 'Bad Request: No valid session ID or initialization request' },
        id: null
      });
      return;
    }

    await transport.handleRequest(req, res, req.body);
  } catch (err) {
    console.error('[mcp] error:', err);
    if (!res.headersSent) {
      res.status(500).json({
        jsonrpc: '2.0',
        error: { code: -32603, message: 'Internal server error' },
        id: null
      });
    }
  }
});

app.listen(PORT, HOST, async () => {
  console.log(`[${new Date().toISOString()}] 🚀 Kommo MCP Server v3.0 on http://${HOST}:${PORT}`);
  console.log(`[${new Date().toISOString()}] 📡 MCP endpoint: http://${HOST}:${PORT}/mcp`);
  console.log(`[${new Date().toISOString()}] 💚 Health: http://${HOST}:${PORT}/health`);
  console.log(`[${new Date().toISOString()}] 🔗 Kommo: ${process.env.KOMMO_BASE_URL}`);
  // Pre-aquece cache
  await refreshCache(true);
});
