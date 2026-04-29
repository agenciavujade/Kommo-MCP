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
// CACHE de pipelines, statuses, usuários
// =====================================================================
interface MetadataCache {
  pipelines: Map<number, KommoPipeline>;
  statuses: Map<number, KommoStatus>;
  users: Map<number, KommoUser>;
  lastRefresh: number;
  refreshing: Promise<void> | null;
}

const cache: MetadataCache = {
  pipelines: new Map(),
  statuses: new Map(),
  users: new Map(),
  lastRefresh: 0,
  refreshing: null,
};

const CACHE_TTL = 10 * 60 * 1000;

async function refreshCache(force = false): Promise<void> {
  if (!force && Date.now() - cache.lastRefresh < CACHE_TTL && cache.pipelines.size > 0) return;
  if (cache.refreshing) return cache.refreshing;

  cache.refreshing = (async () => {
    try {
      const timeout = (ms: number) => new Promise((_, rej) =>
        setTimeout(() => rej(new Error('cache refresh timeout')), ms)
      );

      const [pipelinesRes, usersRes] = await Promise.all([
        Promise.race([kommoAPI.getPipelines(), timeout(15000)]).catch(err => {
          console.error('[cache] pipelines failed:', err.message || err);
          return null;
        }),
        Promise.race([kommoAPI.getUsers(), timeout(15000)]).catch(err => {
          console.error('[cache] users failed:', err.message || err);
          return null;
        }),
      ]);

      if (pipelinesRes) {
        cache.pipelines.clear();
        cache.statuses.clear();
        const pipelines: KommoPipeline[] = (pipelinesRes as any)._embedded?.pipelines || [];
        for (const p of pipelines) {
          cache.pipelines.set(p.id, p);
          const statuses: KommoStatus[] = p._embedded?.statuses || [];
          for (const s of statuses) cache.statuses.set(s.id, s);
        }
      }

      if (usersRes) {
        cache.users.clear();
        const users: KommoUser[] = (usersRes as any)._embedded?.users || [];
        for (const u of users) cache.users.set(u.id, u);
      }

      cache.lastRefresh = Date.now();
      console.log(`[cache] refreshed: ${cache.pipelines.size} pipelines, ${cache.statuses.size} statuses, ${cache.users.size} users`);
    } finally {
      cache.refreshing = null;
    }
  })();

  return cache.refreshing;
}

// =====================================================================
// Enriquecimento
// =====================================================================
function enrichLeadCompact(lead: any): any {
  if (!lead) return lead;
  const status = cache.statuses.get(lead.status_id);
  const pipeline = cache.pipelines.get(lead.pipeline_id);
  const user = cache.users.get(lead.responsible_user_id);

  return {
    id: lead.id,
    name: lead.name,
    price: lead.price,
    status_id: lead.status_id,
    status_name: status?.name || null,
    pipeline_id: lead.pipeline_id,
    pipeline_name: pipeline?.name || null,
    responsible_user_id: lead.responsible_user_id,
    responsible_user_name: user?.name || null,
    is_won: status?.type === 1,
    is_lost: status?.type === 2,
    created_at: lead.created_at,
    updated_at: lead.updated_at,
    closed_at: lead.closed_at || null,
    loss_reason_id: lead.loss_reason_id || null,
  };
}

function enrichLeadVerbose(lead: any): any {
  if (!lead) return lead;
  const compact = enrichLeadCompact(lead);
  return {
    ...compact,
    tags: lead._embedded?.tags || lead.tags || [],
    contacts: lead._embedded?.contacts || lead.contacts || [],
    companies: lead._embedded?.companies || lead.companies || [],
    custom_fields_values: lead.custom_fields_values || null,
    loss_reason: lead._embedded?.loss_reason || null,
  };
}

// =====================================================================
// Helpers
// =====================================================================
function buildLeadsParams(args: any): any {
  const params: any = {};
  if (args.limit) params.limit = Math.min(args.limit, 100);
  if (args.page) params.page = args.page;
  if (args.query) params.query = args.query;
  if (args.order_by) {
    const dir = args.order_dir === 'desc' ? 'desc' : 'asc';
    params[`order[${args.order_by}]`] = dir;
  }
  if (args.with) params.with = args.with;
  if (args.pipeline_id) params['filter[pipeline_id]'] = args.pipeline_id;
  if (args.responsible_user_id) params['filter[responsible_user_id]'] = args.responsible_user_id;

  if (Array.isArray(args.statuses)) {
    args.statuses.forEach((st: any, i: number) => {
      if (st.pipeline_id) params[`filter[statuses][${i}][pipeline_id]`] = st.pipeline_id;
      if (st.status_id) params[`filter[statuses][${i}][status_id]`] = st.status_id;
    });
  }

  if (Array.isArray(args.tags)) {
    args.tags.forEach((tag: string, i: number) => {
      params[`filter[tags][${i}]`] = tag;
    });
  }

  if (args.created_from) params['filter[created_at][from]'] = args.created_from;
  if (args.created_to) params['filter[created_at][to]'] = args.created_to;
  if (args.updated_from) params['filter[updated_at][from]'] = args.updated_from;
  if (args.updated_to) params['filter[updated_at][to]'] = args.updated_to;
  if (args.closed_from) params['filter[closed_at][from]'] = args.closed_from;
  if (args.closed_to) params['filter[closed_at][to]'] = args.closed_to;
  return params;
}

function toUnixTimestamp(input: any): number | undefined {
  if (input == null) return undefined;
  if (typeof input === 'number') return input > 1e12 ? Math.floor(input / 1000) : input;
  const d = new Date(input);
  if (isNaN(d.getTime())) return undefined;
  return Math.floor(d.getTime() / 1000);
}

// Constrói o objeto de update aceitando todos os campos suportados
function buildLeadUpdatePayload(a: any): any {
  const update: any = {};
  ['name', 'price', 'status_id', 'pipeline_id', 'responsible_user_id', 'loss_reason_id'].forEach(k => {
    if (a[k] != null) update[k] = a[k];
  });
  // Custom fields: aceita array no formato { field_id, values: [{value}] } ou simplificado { field_id, value }
  if (Array.isArray(a.custom_fields_values)) {
    update.custom_fields_values = a.custom_fields_values.map((cf: any) => {
      if (cf.values) return { field_id: cf.field_id, values: cf.values };
      if (cf.value !== undefined) return { field_id: cf.field_id, values: [{ value: cf.value }] };
      return cf;
    });
  }
  return update;
}

// =====================================================================
// MCP Server
// =====================================================================
function createMcpServer(): Server {
  const server = new Server(
    { name: 'kommo-mcp-server', version: '3.3.0' },
    { capabilities: { tools: {} } }
  );

  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: [
      // ============ LEITURA ============
      {
        name: 'get_leads',
        description: 'Lista leads com filtros completos. Retorna até 25 leads em modo compacto por padrão. Use closed_from/closed_to para "vendas no período X". Aceita filter por tags, pipeline, status, vendedor, datas. Para apenas contar, use count_leads.',
        inputSchema: {
          type: 'object',
          properties: {
            limit: { type: 'number', description: 'Até 100, padrão 25' },
            page: { type: 'number' },
            query: { type: 'string' },
            pipeline_id: { type: 'number' },
            responsible_user_id: { type: 'number' },
            statuses: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  pipeline_id: { type: 'number' },
                  status_id: { type: 'number' }
                }
              }
            },
            tags: { type: 'array', items: { type: 'string' }, description: 'Filtrar por nomes de tags' },
            created_from: { type: 'string' },
            created_to: { type: 'string' },
            updated_from: { type: 'string' },
            updated_to: { type: 'string' },
            closed_from: { type: 'string' },
            closed_to: { type: 'string' },
            order_by: { type: 'string', enum: ['created_at', 'updated_at', 'id'] },
            order_dir: { type: 'string', enum: ['asc', 'desc'] },
            verbose: { type: 'boolean' }
          }
        }
      },
      {
        name: 'count_leads',
        description: 'Conta leads (sem retornar a lista) que batem com os filtros. Aceita os mesmos filtros do get_leads. Retorna total_count, total_value, average_value.',
        inputSchema: {
          type: 'object',
          properties: {
            pipeline_id: { type: 'number' },
            responsible_user_id: { type: 'number' },
            statuses: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  pipeline_id: { type: 'number' },
                  status_id: { type: 'number' }
                }
              }
            },
            tags: { type: 'array', items: { type: 'string' } },
            created_from: { type: 'string' },
            created_to: { type: 'string' },
            closed_from: { type: 'string' },
            closed_to: { type: 'string' },
            query: { type: 'string' },
            max_pages: { type: 'number', description: 'Padrão 20 (até 5000 leads)' }
          }
        }
      },
      {
        name: 'get_lead_by_id',
        description: 'Detalhe completo de um lead.',
        inputSchema: {
          type: 'object',
          properties: {
            id: { type: 'number' },
            with: { type: 'string', description: 'Ex: contacts,loss_reason,catalog_elements,tags' }
          },
          required: ['id']
        }
      },
      {
        name: 'get_lead_notes',
        description: 'Notas (anotações) de um lead.',
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
        description: 'Histórico de eventos de um lead (mudanças de status, etc).',
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
        name: 'get_lead_messages',
        description: 'Histórico de mensagens (chats) vinculadas a um lead. Depende das integrações de chat ativas no Kommo (WhatsApp, Telegram, etc). Retorna talks (conversas) e mensagens. Se a conta não usa chat pelo Kommo, virá vazio.',
        inputSchema: {
          type: 'object',
          properties: {
            lead_id: { type: 'number' },
            messages_per_chat: { type: 'number', description: 'Quantas mensagens carregar por conversa. Padrão 50.' }
          },
          required: ['lead_id']
        }
      },
      {
        name: 'get_events',
        description: 'Eventos do CRM. Para histórico de fechamentos: type=lead_status_changed.',
        inputSchema: {
          type: 'object',
          properties: {
            type: { type: 'string' },
            entity_type: { type: 'string', enum: ['lead', 'contact', 'company'] },
            entity_id: { type: 'number' },
            created_from: { type: 'string' },
            created_to: { type: 'string' },
            limit: { type: 'number' },
            page: { type: 'number' }
          }
        }
      },
      {
        name: 'get_pipelines',
        description: 'Lista os funis. Por padrão compacto. Passe include_statuses=true para ver as etapas.',
        inputSchema: {
          type: 'object',
          properties: {
            include_statuses: { type: 'boolean' }
          }
        }
      },
      {
        name: 'get_pipeline_statuses',
        description: 'Etapas de um funil específico.',
        inputSchema: {
          type: 'object',
          properties: { pipeline_id: { type: 'number' } },
          required: ['pipeline_id']
        }
      },
      {
        name: 'get_users',
        description: 'Lista usuários (vendedoras) da conta.',
        inputSchema: { type: 'object', properties: {} }
      },
      {
        name: 'get_loss_reasons',
        description: 'Motivos de perda configurados.',
        inputSchema: { type: 'object', properties: {} }
      },
      {
        name: 'get_contacts',
        description: 'Lista contatos.',
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
        description: 'Lista empresas.',
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
        description: 'Lista tarefas.',
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
        description: 'Definições de custom fields de leads.',
        inputSchema: { type: 'object', properties: {} }
      },

      // ============ ESCRITA ============
      {
        name: 'create_lead',
        description: 'Cria um novo lead.',
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
        description: 'Atualiza qualquer campo de um lead: nome, valor, status, pipeline, responsável, motivo de perda e custom fields. Custom fields no formato [{field_id, value}] ou [{field_id, values:[{value}]}].',
        inputSchema: {
          type: 'object',
          properties: {
            id: { type: 'number' },
            name: { type: 'string' },
            price: { type: 'number' },
            status_id: { type: 'number' },
            pipeline_id: { type: 'number' },
            responsible_user_id: { type: 'number' },
            loss_reason_id: { type: 'number' },
            custom_fields_values: {
              type: 'array',
              description: 'Array de {field_id, value} ou {field_id, values:[{value}]}',
              items: { type: 'object' }
            }
          },
          required: ['id']
        }
      },
      {
        name: 'update_leads_bulk',
        description: 'Atualiza vários leads de uma vez (até 250 por chamada). Cada item do array deve ter id e os campos a atualizar.',
        inputSchema: {
          type: 'object',
          properties: {
            leads: {
              type: 'array',
              description: 'Array de leads no formato {id, ...campos a atualizar}. Máximo 250.',
              items: { type: 'object' }
            }
          },
          required: ['leads']
        }
      },
      {
        name: 'move_lead_status',
        description: 'Atalho para mover um lead para outro status (e opcionalmente outro pipeline).',
        inputSchema: {
          type: 'object',
          properties: {
            lead_id: { type: 'number' },
            status_id: { type: 'number' },
            pipeline_id: { type: 'number' }
          },
          required: ['lead_id', 'status_id']
        }
      },
      {
        name: 'add_note_to_lead',
        description: 'Adiciona nota a um lead.',
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
        name: 'add_tags_to_lead',
        description: 'Adiciona tags a um lead (mantém as existentes e acrescenta as novas).',
        inputSchema: {
          type: 'object',
          properties: {
            lead_id: { type: 'number' },
            tags: { type: 'array', items: { type: 'string' } }
          },
          required: ['lead_id', 'tags']
        }
      },
      {
        name: 'remove_tags_from_lead',
        description: 'Remove tags específicas de um lead, preservando as outras.',
        inputSchema: {
          type: 'object',
          properties: {
            lead_id: { type: 'number' },
            tags: { type: 'array', items: { type: 'string' } }
          },
          required: ['lead_id', 'tags']
        }
      },
      {
        name: 'set_lead_tags',
        description: 'Substitui completamente as tags do lead pelas informadas. Passe array vazio para limpar todas.',
        inputSchema: {
          type: 'object',
          properties: {
            lead_id: { type: 'number' },
            tags: { type: 'array', items: { type: 'string' } }
          },
          required: ['lead_id', 'tags']
        }
      },
      {
        name: 'create_task',
        description: 'Cria uma tarefa.',
        inputSchema: {
          type: 'object',
          properties: {
            text: { type: 'string' },
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
        description: 'Conclui uma tarefa.',
        inputSchema: {
          type: 'object',
          properties: {
            task_id: { type: 'number' },
            result_text: { type: 'string' }
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
      await refreshCache(false);
      let payload: any;

      switch (name) {
        case 'get_leads': {
          const filterArgs = { ...a };
          ['created_from', 'created_to', 'updated_from', 'updated_to', 'closed_from', 'closed_to'].forEach(k => {
            if (filterArgs[k] != null) {
              const ts = toUnixTimestamp(filterArgs[k]);
              if (ts !== undefined) filterArgs[k] = ts;
            }
          });
          if (!filterArgs.limit) filterArgs.limit = 25;

          const params = buildLeadsParams(filterArgs);
          const data = await kommoAPI.getLeads(params);
          const rawLeads = data._embedded?.leads || [];
          const enricher = a.verbose ? enrichLeadVerbose : enrichLeadCompact;
          const leads = rawLeads.map(enricher);

          payload = {
            count: leads.length,
            page: filterArgs.page || 1,
            has_next: !!data._links?.next,
            leads,
          };
          break;
        }

        case 'count_leads': {
          const filterArgs = { ...a };
          ['created_from', 'created_to', 'updated_from', 'updated_to', 'closed_from', 'closed_to'].forEach(k => {
            if (filterArgs[k] != null) {
              const ts = toUnixTimestamp(filterArgs[k]);
              if (ts !== undefined) filterArgs[k] = ts;
            }
          });

          const maxPages = a.max_pages || 20;
          const limit = 250;
          let page = 1;
          let total = 0;
          let totalValue = 0;
          let hasNext = true;

          while (hasNext && page <= maxPages) {
            const params = buildLeadsParams({ ...filterArgs, limit, page });
            const data = await kommoAPI.getLeads(params);
            const leads = data._embedded?.leads || [];
            total += leads.length;
            totalValue += leads.reduce((sum: number, l: any) => sum + (l.price || 0), 0);
            hasNext = !!data._links?.next;
            page++;
            if (hasNext) await new Promise(res => setTimeout(res, 150));
          }

          payload = {
            total_count: total,
            total_value: totalValue,
            average_value: total > 0 ? totalValue / total : 0,
            pages_scanned: page - 1,
            reached_max_pages: page > maxPages && hasNext,
          };
          break;
        }

        case 'get_lead_by_id': {
          const lead = await kommoAPI.getLead(a.id, a.with);
          payload = enrichLeadVerbose(lead);
          break;
        }

        case 'get_lead_notes': {
          const params: any = { limit: a.limit || 25 };
          if (a.page) params.page = a.page;
          payload = await kommoAPI.getLeadNotes(a.lead_id, params);
          break;
        }

        case 'get_lead_events': {
          const params: any = { limit: a.limit || 25 };
          payload = await kommoAPI.getLeadEvents(a.lead_id, params);
          break;
        }

        case 'get_lead_messages': {
          // 1) Lista talks (conversas) vinculadas ao lead
          const messagesPerChat = a.messages_per_chat || 50;
          const talksData = await kommoAPI.getLeadTalks(a.lead_id).catch(err => {
            console.error('[get_lead_messages] talks failed:', err);
            return null;
          });

          const talks = (talksData as any)?._embedded?.talks || [];
          const result: any = {
            lead_id: a.lead_id,
            talks_count: talks.length,
            conversations: [],
            note: talks.length === 0
              ? 'Nenhuma conversa de chat encontrada para este lead. Pode ser que o cliente não use chat integrado ao Kommo, ou as conversas estejam em outro contato/empresa.'
              : undefined,
          };

          // 2) Para cada talk, busca mensagens do chat
          for (const talk of talks) {
            const chatId = talk.chat_id;
            if (!chatId) continue;
            try {
              const msgs = await kommoAPI.getChatMessages(chatId, { limit: messagesPerChat });
              const messages = (msgs as any)?._embedded?.messages || (msgs as any)?.messages || [];
              result.conversations.push({
                talk_id: talk.id,
                chat_id: chatId,
                contact_id: talk.contact_id || null,
                origin: talk.origin || null,
                messages_count: messages.length,
                messages: messages.map((m: any) => ({
                  id: m.id,
                  type: m.type,
                  text: m.text || m.message || null,
                  author_name: m.author?.name || null,
                  author_type: m.author?.type || null,
                  created_at: m.created_at,
                })),
              });
            } catch (chatErr) {
              const e = KommoAPI.formatError(chatErr);
              result.conversations.push({
                talk_id: talk.id,
                chat_id: chatId,
                error: e.message,
                error_status: e.status,
              });
            }
          }

          payload = result;
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
          params.limit = a.limit || 25;
          if (a.page) params.page = a.page;
          payload = await kommoAPI.getEvents(params);
          break;
        }

        case 'get_pipelines': {
          const pipelinesArr = Array.from(cache.pipelines.values());
          if (a.include_statuses) {
            payload = {
              count: pipelinesArr.length,
              pipelines: pipelinesArr.map(p => ({
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
                  sort: s.sort,
                }))
              }))
            };
          } else {
            payload = {
              count: pipelinesArr.length,
              pipelines: pipelinesArr.map(p => ({
                id: p.id,
                name: p.name,
                is_main: p.is_main,
                is_archive: p.is_archive,
                statuses_count: (p._embedded?.statuses || []).length,
              })),
              hint: 'Use include_statuses=true para ver as etapas, ou get_pipeline_statuses(pipeline_id).'
            };
          }
          break;
        }

        case 'get_pipeline_statuses': {
          const pipeline = cache.pipelines.get(a.pipeline_id);
          if (pipeline?._embedded?.statuses) {
            payload = {
              pipeline_id: a.pipeline_id,
              pipeline_name: pipeline.name,
              count: pipeline._embedded.statuses.length,
              statuses: pipeline._embedded.statuses.map(s => ({
                id: s.id,
                name: s.name,
                type: s.type,
                is_won: s.type === 1,
                is_lost: s.type === 2,
                sort: s.sort,
              }))
            };
          } else {
            payload = await kommoAPI.getPipelineStatuses(a.pipeline_id);
          }
          break;
        }

        case 'get_users': {
          if (cache.users.size > 0) {
            const users = Array.from(cache.users.values()).map(u => ({
              id: u.id,
              name: u.name,
              email: u.email,
            }));
            payload = { count: users.length, users };
          } else {
            const data = await kommoAPI.getUsers();
            const users = (data._embedded?.users || []).map((u: any) => ({
              id: u.id,
              name: u.name,
              email: u.email,
            }));
            payload = { count: users.length, users };
          }
          break;
        }

        case 'get_loss_reasons': {
          payload = await kommoAPI.getLossReasons();
          break;
        }

        case 'get_contacts': {
          const params: any = { limit: a.limit || 25, page: a.page || 1 };
          if (a.query) params.query = a.query;
          payload = await kommoAPI.getContacts(params);
          break;
        }

        case 'get_companies': {
          const params: any = { limit: a.limit || 25, page: a.page || 1 };
          if (a.query) params.query = a.query;
          payload = await kommoAPI.getCompanies(params);
          break;
        }

        case 'get_tasks': {
          const params: any = { limit: a.limit || 25, page: a.page || 1 };
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

        // ============ ESCRITA ============
        case 'create_lead': {
          const leadPayload: any = { name: a.name };
          if (a.price != null) leadPayload.price = a.price;
          if (a.status_id) leadPayload.status_id = a.status_id;
          if (a.pipeline_id) leadPayload.pipeline_id = a.pipeline_id;
          if (a.responsible_user_id) leadPayload.responsible_user_id = a.responsible_user_id;
          if (Array.isArray(a.tags)) leadPayload._embedded = { tags: a.tags.map((t: string) => ({ name: t })) };
          payload = enrichLeadCompact(await kommoAPI.createLead(leadPayload));
          break;
        }

        case 'update_lead': {
          const update = buildLeadUpdatePayload(a);
          payload = enrichLeadCompact(await kommoAPI.updateLead(a.id, update));
          break;
        }

        case 'update_leads_bulk': {
          if (!Array.isArray(a.leads)) throw new Error('Parâmetro leads deve ser um array');
          if (a.leads.length === 0) throw new Error('Array leads vazio');
          if (a.leads.length > 250) throw new Error('Máximo 250 leads por chamada');
          // Cada lead precisa ter id; os demais campos são livres
          const result = await kommoAPI.updateLeadsBulk(a.leads);
          payload = {
            updated_count: a.leads.length,
            result,
          };
          break;
        }

        case 'move_lead_status': {
          const update: any = { status_id: a.status_id };
          if (a.pipeline_id) update.pipeline_id = a.pipeline_id;
          payload = enrichLeadCompact(await kommoAPI.updateLead(a.lead_id, update));
          break;
        }

        case 'add_note_to_lead': {
          payload = await kommoAPI.addNoteToLead(a.lead_id, a.text);
          break;
        }

        case 'add_tags_to_lead': {
          if (!Array.isArray(a.tags) || a.tags.length === 0) throw new Error('tags deve ser array não vazio');
          // Mantém as tags atuais e adiciona as novas
          const lead = await kommoAPI.getLead(a.lead_id, 'tags');
          const current = (lead._embedded?.tags || []).map((t: any) => t.name).filter(Boolean);
          const merged = Array.from(new Set([...current, ...a.tags]));
          payload = enrichLeadCompact(await kommoAPI.setLeadTags(a.lead_id, merged));
          break;
        }

        case 'remove_tags_from_lead': {
          if (!Array.isArray(a.tags) || a.tags.length === 0) throw new Error('tags deve ser array não vazio');
          payload = enrichLeadCompact(await kommoAPI.removeTagsFromLead(a.lead_id, a.tags));
          break;
        }

        case 'set_lead_tags': {
          if (!Array.isArray(a.tags)) throw new Error('tags deve ser array (pode ser vazio para limpar)');
          payload = enrichLeadCompact(await kommoAPI.setLeadTags(a.lead_id, a.tags));
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
          hint: e.status === 401 ? 'Token expirado ou inválido' :
                e.status === 403 ? 'Sem permissão' :
                e.status === 404 ? 'Recurso não encontrado' :
                e.status === 429 ? 'Rate limit (7 req/s)' :
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
    version: '3.3.0',
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
  console.log(`[${new Date().toISOString()}] 🚀 Kommo MCP Server v3.3 on http://${HOST}:${PORT}`);
  console.log(`[${new Date().toISOString()}] 📡 MCP endpoint: http://${HOST}:${PORT}/mcp`);
  console.log(`[${new Date().toISOString()}] 💚 Health: http://${HOST}:${PORT}/health`);
  console.log(`[${new Date().toISOString()}] 🔗 Kommo: ${process.env.KOMMO_BASE_URL}`);
  refreshCache(true).catch(err => console.error('[boot] cache preheat failed:', err));
});
