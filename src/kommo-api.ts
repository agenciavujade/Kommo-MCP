import axios, { AxiosInstance, AxiosError } from 'axios';

export interface KommoConfig {
  baseUrl: string;
  accessToken: string;
}

export interface KommoLead {
  id: number;
  name: string;
  price: number;
  status_id: number;
  pipeline_id: number;
  created_at: number;
  updated_at: number;
  responsible_user_id: number;
  created_by: number;
  closed_at?: number;
  loss_reason_id?: number;
  source_id?: number;
  tags?: string[];
  contacts?: KommoContact[];
  companies?: KommoCompany[];
  custom_fields_values?: any[];
}

export interface KommoContact {
  id: number;
  name: string;
  first_name: string;
  last_name: string;
  responsible_user_id: number;
  created_by: number;
  created_at: number;
  updated_at: number;
  custom_fields_values?: any[];
  tags?: string[];
  leads?: KommoLead[];
  companies?: KommoCompany[];
}

export interface KommoCompany {
  id: number;
  name: string;
  responsible_user_id: number;
  created_by: number;
  created_at: number;
  updated_at: number;
  custom_fields_values?: any[];
  tags?: string[];
  leads?: KommoLead[];
  contacts?: KommoContact[];
}

export interface KommoPipeline {
  id: number;
  name: string;
  sort: number;
  is_main: boolean;
  is_unsorted_on: boolean;
  is_archive: boolean;
  account_id: number;
  _embedded?: {
    statuses?: KommoStatus[];
  };
  _links?: any;
}

export interface KommoTask {
  id: number;
  text: string;
  entity_id: number;
  entity_type: string;
  responsible_user_id: number;
  created_by: number;
  created_at: number;
  updated_at: number;
  complete_till: number;
  is_completed?: boolean;
  task_type_id?: number;
  result?: { text: string };
}

export interface KommoNote {
  id: number;
  entity_id: number;
  created_by: number;
  created_at: number;
  updated_at: number;
  responsible_user_id: number;
  group_id?: number;
  note_type: string;
  params?: any;
}

export interface KommoEvent {
  id: string;
  type: string;
  entity_id: number;
  entity_type: string;
  created_by: number;
  created_at: number;
  value_after?: any;
  value_before?: any;
}

export interface KommoStatus {
  id: number;
  name: string;
  sort: number;
  is_editable?: boolean;
  pipeline_id: number;
  color: string;
  type: number; // 0 = normal, 1 = won, 2 = lost
  account_id: number;
}

export interface KommoLossReason {
  id: number;
  name: string;
  sort: number;
  is_editable?: boolean;
}

export interface KommoUser {
  id: number;
  name: string;
  email: string;
  lang: string;
  rights: any;
}

export class KommoAPI {
  private client: AxiosInstance;

  constructor(config: KommoConfig) {
    this.client = axios.create({
      baseURL: config.baseUrl,
      headers: {
        'Authorization': `Bearer ${config.accessToken}`,
        'Content-Type': 'application/json',
      },
      timeout: 30000,
    });
  }

  // ===== Account =====
  async getAccount(): Promise<any> {
    const r = await this.client.get('/api/v4/account');
    return r.data;
  }

  // ===== Leads =====
  async getLeads(params?: any): Promise<any> {
    const r = await this.client.get('/api/v4/leads', { params });
    return r.data;
  }

  async getLead(id: number, withParam?: string): Promise<KommoLead> {
    const params: any = {};
    if (withParam) params.with = withParam;
    const r = await this.client.get(`/api/v4/leads/${id}`, { params });
    return r.data;
  }

  async getAllLeads(params?: any): Promise<KommoLead[]> {
    const allLeads: KommoLead[] = [];
    let page = 1;
    let hasMore = true;
    const limit = 250;
    let consecutiveEmptyPages = 0;
    const maxEmptyPages = 2;
    const maxPages = 50; // safety cap (12.5k leads max)

    while (hasMore && consecutiveEmptyPages < maxEmptyPages && page <= maxPages) {
      try {
        const r = await this.client.get('/api/v4/leads', {
          params: { ...params, limit, page }
        });
        const leads = r.data._embedded?.leads || [];
        if (leads.length === 0) {
          consecutiveEmptyPages++;
        } else {
          consecutiveEmptyPages = 0;
          allLeads.push(...leads);
        }
        page++;
        hasMore = !!r.data._links?.next;
        if (hasMore) await new Promise(res => setTimeout(res, 150)); // rate-limit safety
      } catch (err) {
        console.error(`Erro paginação leads página ${page}:`, err);
        break;
      }
    }
    return allLeads;
  }

  async createLead(lead: Partial<KommoLead>): Promise<KommoLead> {
    const r = await this.client.post('/api/v4/leads', [lead]);
    return r.data._embedded.leads[0];
  }

  async updateLead(id: number, lead: Partial<KommoLead>): Promise<KommoLead> {
    const r = await this.client.patch(`/api/v4/leads/${id}`, lead);
    return r.data;
  }

  // ===== Contacts =====
  async getContacts(params?: any): Promise<any> {
    const r = await this.client.get('/api/v4/contacts', { params });
    return r.data;
  }

  async getContact(id: number): Promise<KommoContact> {
    const r = await this.client.get(`/api/v4/contacts/${id}`);
    return r.data;
  }

  async createContact(contact: Partial<KommoContact>): Promise<KommoContact> {
    const r = await this.client.post('/api/v4/contacts', [contact]);
    return r.data._embedded.contacts[0];
  }

  async updateContact(id: number, contact: Partial<KommoContact>): Promise<KommoContact> {
    const r = await this.client.patch(`/api/v4/contacts/${id}`, contact);
    return r.data;
  }

  // ===== Companies =====
  async getCompanies(params?: any): Promise<any> {
    const r = await this.client.get('/api/v4/companies', { params });
    return r.data;
  }

  async getCompany(id: number): Promise<KommoCompany> {
    const r = await this.client.get(`/api/v4/companies/${id}`);
    return r.data;
  }

  async createCompany(company: Partial<KommoCompany>): Promise<KommoCompany> {
    const r = await this.client.post('/api/v4/companies', [company]);
    return r.data._embedded.companies[0];
  }

  async updateCompany(id: number, company: Partial<KommoCompany>): Promise<KommoCompany> {
    const r = await this.client.patch(`/api/v4/companies/${id}`, company);
    return r.data;
  }

  // ===== Pipelines + Statuses =====
  async getPipelines(): Promise<any> {
    const r = await this.client.get('/api/v4/leads/pipelines');
    return r.data;
  }

  async getPipeline(id: number): Promise<KommoPipeline> {
    const r = await this.client.get(`/api/v4/leads/pipelines/${id}`);
    return r.data;
  }

  async getPipelineStatuses(pipelineId: number): Promise<any> {
    const r = await this.client.get(`/api/v4/leads/pipelines/${pipelineId}/statuses`);
    return r.data;
  }

  // ===== Tasks =====
  async getTasks(params?: any): Promise<any> {
    const r = await this.client.get('/api/v4/tasks', { params });
    return r.data;
  }

  async getTask(id: number): Promise<KommoTask> {
    const r = await this.client.get(`/api/v4/tasks/${id}`);
    return r.data;
  }

  async createTask(task: Partial<KommoTask>): Promise<KommoTask> {
    const r = await this.client.post('/api/v4/tasks', [task]);
    return r.data._embedded.tasks[0];
  }

  async updateTask(id: number, task: Partial<KommoTask>): Promise<KommoTask> {
    const r = await this.client.patch(`/api/v4/tasks/${id}`, task);
    return r.data;
  }

  async completeTask(id: number, resultText?: string): Promise<KommoTask> {
    const payload: any = { is_completed: true };
    if (resultText) payload.result = { text: resultText };
    const r = await this.client.patch(`/api/v4/tasks/${id}`, payload);
    return r.data;
  }

  // ===== Users =====
  async getUsers(): Promise<any> {
    const r = await this.client.get('/api/v4/users');
    return r.data;
  }

  async getUser(id: number): Promise<KommoUser> {
    const r = await this.client.get(`/api/v4/users/${id}`);
    return r.data;
  }

  // ===== Events =====
  async getEvents(params?: any): Promise<any> {
    const r = await this.client.get('/api/v4/events', { params });
    return r.data;
  }

  async getLeadEvents(leadId: number, params?: any): Promise<any> {
    const r = await this.client.get(`/api/v4/leads/${leadId}/events`, { params });
    return r.data;
  }

  // ===== Notes =====
  async getLeadNotes(leadId: number, params?: any): Promise<any> {
    const r = await this.client.get(`/api/v4/leads/${leadId}/notes`, { params });
    return r.data;
  }

  async addNoteToLead(leadId: number, text: string, noteType: string = 'common'): Promise<KommoNote> {
    const payload = [{ note_type: noteType, params: { text } }];
    const r = await this.client.post(`/api/v4/leads/${leadId}/notes`, payload);
    return r.data._embedded.notes[0];
  }

  async pinNote(entityType: 'leads' | 'contacts' | 'companies', noteId: number): Promise<any> {
    const r = await this.client.post(`/api/v4/${entityType}/notes/${noteId}/pin`);
    return r.data;
  }

  async unpinNote(entityType: 'leads' | 'contacts' | 'companies', noteId: number): Promise<any> {
    const r = await this.client.post(`/api/v4/${entityType}/notes/${noteId}/unpin`);
    return r.data;
  }

  // ===== Loss Reasons =====
  async getLossReasons(): Promise<any> {
    const r = await this.client.get('/api/v4/leads/loss_reasons');
    return r.data;
  }

  // ===== Custom Fields =====
  async getLeadsCustomFields(): Promise<any> {
    const r = await this.client.get('/api/v4/leads/custom_fields');
    return r.data;
  }

  async getContactsCustomFields(): Promise<any> {
    const r = await this.client.get('/api/v4/contacts/custom_fields');
    return r.data;
  }

  async getCompaniesCustomFields(): Promise<any> {
    const r = await this.client.get('/api/v4/companies/custom_fields');
    return r.data;
  }

  // ===== Salesbot =====
  async runSalesbot(params: { entity_id: number; entity_type: string; [k: string]: any }): Promise<any> {
    const r = await this.client.post('/api/v4/bots/run', params);
    return r.data;
  }

  async stopSalesbot(botId: number): Promise<any> {
    const r = await this.client.post(`/api/v4/bots/${botId}/stop`);
    return r.data;
  }

  // ===== Helper para extrair erro estruturado =====
  static formatError(err: unknown): { status?: number; message: string; detail?: any } {
    if (err instanceof AxiosError) {
      return {
        status: err.response?.status,
        message: err.message,
        detail: err.response?.data,
      };
    }
    return { message: err instanceof Error ? err.message : String(err) };
  }
}
