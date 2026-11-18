<template>
  <section class="page" data-module="transform">
    <header class="page-head">
      <div><h2>采集转换管道</h2><p class="page-desc">维护转换节点，围绕节点编号、输入来源、输出目标、转换表达式做登记、筛选与状态流转。列表、拓扑详情与血缘读取共用同一字段投影版本与可用标记。</p>
        <p class="projection-tag" data-testid="projection-version" v-if="graph.projection_version">字段投影版本 v{{ graph.projection_version }}（{{ graph.projection_mode === 'latest' ? '提交时刻最新版本' : '读取开始冻结版本' }}）</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记转换节点</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出关系清单</button>
      </div>
    </header>
    <div class="dag-wrap" ref="dagRef">
      <svg class="dag-svg" :viewBox="`0 0 ${svgW} ${svgH}`">
        <defs><marker id="arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#94a3b8"/></marker></defs>
        <g v-for="edge in graph.edges" :key="edge.from+edge.to">
          <line class="dag-edge" :x1="nodePos[edge.from]?.x||0" :y1="nodePos[edge.from]?.y||0" :x2="nodePos[edge.to]?.x||0" :y2="nodePos[edge.to]?.y||0"/>
          <text :x="(nodePos[edge.from]?.x+nodePos[edge.to]?.x)/2" :y="(nodePos[edge.from]?.y+nodePos[edge.to]?.y)/2 - 6" font-size="10" fill="#64748b" text-anchor="middle">{{ edge.label }}</text>
        </g>
        <g v-for="node in graph.nodes" :key="node.id" :class="['dag-node', {selected: selectedNode?.id===node.id, disabled: node.可用标记===false}]" @click="selectNode(node)" @mousedown="startDrag($event, node)">
          <rect :x="nodePos[node.id]?.x - 60" :y="nodePos[node.id]?.y - 20" width="120" height="40" rx="6" :fill="nodeColor(node.status)"/>
          <text :x="nodePos[node.id]?.x" :y="nodePos[node.id]?.y + 4" text-anchor="middle">{{ node.name || node.label }}</text>
          <text :x="nodePos[node.id]?.x + 52" :y="nodePos[node.id]?.y - 10" font-size="9" :fill="node.可用标记 ? '#059669' : '#dc2626'" text-anchor="middle">{{ node.可用标记 ? '可用' : '停用' }}</text>
        </g>
      </svg>
    </div>
    <div v-if="selectedNode" class="dag-detail" data-testid="dag-detail">
      <dl><dt>编号</dt><dd>{{ selectedNode.id }}</dd></dl>
      <dl><dt>名称</dt><dd>{{ selectedNode.name || selectedNode.label }}</dd></dl>
      <dl><dt>可用标记</dt><dd data-testid="availability-flag">{{ selectedNode.可用标记 ? '可用（可被引用）' : '不可用（停用/未启用）' }}</dd></dl>
      <dl><dt>关系版本</dt><dd>{{ selectedNode.关系版本 ?? '—' }}</dd></dl>
      <dl v-for="f in detailFields" :key="f"><dt>{{ f }}</dt><dd>{{ selectedNode[f] ?? '—' }}</dd></dl>
      <dl><dt>字段投影版本</dt><dd>v{{ selectedNode.投影版本 ?? graph.projection_version }}</dd></dl>
      <div style="grid-column:1/-1; margin-top:8px; display:flex; gap:6px;">
        <button v-for="action in actions" :key="action" class="btn" :data-testid="`action-${selectedNode.id}-${action}`" @click="runAction(action, selectedNode)">{{ action }}</button>
      </div>
    </div>
    <section v-if="graph.relations?.length" class="relation-panel" data-testid="relation-panel">
      <h3>转换节点关系字段记录（关系版本留痕，历史版本不与当前版本混用）</h3>
      <table class="data-table">
        <thead><tr><th>节点编号</th><th>可用标记</th><th>输入来源</th><th>输出目标</th><th>关系版本</th><th>清单类型</th></tr></thead>
        <tbody>
          <tr v-for="r in graph.relations" :key="r.关系版本" :data-list-type="r.清单类型">
            <td>{{ r.节点编号 }}</td><td>{{ r.可用标记 ? '可用' : '不可用' }}</td>
            <td>{{ r.输入来源 }}</td><td>{{ r.输出目标 }}</td><td>{{ r.关系版本 }}</td>
            <td>{{ r.清单类型 === 'executable' ? '可执行清单' : r.清单类型 === 'history' ? '历史关系清单' : '待校验集合' }}</td>
          </tr>
        </tbody>
      </table>
      <p v-if="graph.anomalies?.length" class="anomaly-tip" data-testid="anomaly-tip">依赖异常 {{ graph.anomalies.length }} 条：{{ graph.anomalies.map((a: any) => `${a.节点编号}→${a.依赖节点}（${a.异常类型}）`).join('；') }}</p>
    </section>
    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记转换节点</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
    <footer class="page-foot"><span>{{ graph.nodes.length }} 个节点</span></footer>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/transform'
const columns = ["节点编号", "输入来源", "输出目标", "转换表达式", "执行顺序", "依赖节点", "失败策略", "节点状态"]
const actions = ["提交校验", "启用节点", "停用节点"]
const statuses = ["草稿", "待校验", "已启用", "已停用"]
const STATUS_COLORS: Record<string,string> = { '草稿':'#e2e8f0','待校验':'#fef3c7','已启用':'#d1fae5','已停用':'#fee2e2' }
const nodeColor = (s: string) => STATUS_COLORS[s] || '#e2e8f0'
const graph = reactive<Record<string, any>>({ nodes: [], edges: [] })
const nodePos = ref<Record<number,{x:number;y:number}>>({})
const selectedNode = ref<any>(null)
const detailFields = computed(() => columns.filter((f: string) => !['id','name','label','status'].includes(f)))
const svgW = ref(800); const svgH = ref(400)
const errorMessage = ref('')
const creating = ref(false); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
let dragNode: any = null; const dagRef = ref<HTMLElement|null>(null)
function layout(nodes: any[]) {
  const w = 800; const h = Math.max(400, nodes.length * 80)
  svgW.value = w; svgH.value = h
  const pos: Record<number,{x:number;y:number}> = {}
  nodes.forEach((n,i) => { pos[n.id] = { x: 120 + (i % 5) * 150, y: 50 + Math.floor(i / 5) * 80 } })
  nodePos.value = pos
}
function selectNode(node: any) { selectedNode.value = selectedNode.value?.id === node.id ? null : node }
function startDrag(e: MouseEvent, node: any) { dragNode = node; const onMove = (ev: MouseEvent) => { if(!dragNode) return; nodePos.value[node.id] = { x: ev.offsetX, y: ev.offsetY } }; const onUp = () => { dragNode = null; document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp) }; document.addEventListener('mousemove', onMove); document.addEventListener('mouseup', onUp) }
async function reload() {
  try {
    const res = await request(ENDPOINT)
    const data = await res.json()
    // 整体接收版本化字段投影：节点、关系记录、异常清单、核对记录同源同版本。
    Object.assign(graph, data)
    graph.nodes = data.nodes || []
    graph.edges = data.edges || []
    layout(graph.nodes)
  } catch(e) { errorMessage.value = '加载失败' }
}
async function runAction(action: string, node: any) {
  try { await request(`${ENDPOINT}/${node.id}/actions`, { method: 'POST', body: JSON.stringify({ action }) }); await reload() } catch(e) { errorMessage.value = '操作失败' }
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
onMounted(reload)
</script>
