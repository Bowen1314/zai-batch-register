const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

const sourceRoot = process.env.REGRESSION_SOURCE_ROOT || path.join(__dirname, '..');
const source = fs.readFileSync(path.join(sourceRoot, 'static/app.js'), 'utf8');

function page(fetch) {
  const nodes = {};
  const context = vm.createContext({
    window: { addEventListener() {} },
    document: { getElementById(id) {
      return nodes[id] ||= {
        value: 'fixture', checked: true, innerHTML: '',
        classList: { add() {}, remove() {}, toggle() {} },
      };
    } },
    fetch,
  });
  vm.runInContext(source, context);
  return { context, nodes };
}

for (const success of [true, false]) {
  test(`email test escapes server HTML for success=${success}`, async () => {
    const marker = '<b data-audit="marker">AUDIT_MARKER</b>';
    const { context, nodes } = page(async () => ({ json: async () => ({ success, message: marker }) }));
    await vm.runInContext('testEmailConnection()', context);
    assert.ok(nodes.emailTestResult.innerHTML.includes('&lt;b'));
    assert.ok(!nodes.emailTestResult.innerHTML.includes(marker));
    assert.equal(nodes.btnTestEmail.disabled, false);
  });
}

test('email test escapes network errors', async () => {
  const { context, nodes } = page(async () => { throw new Error('<b>network</b>'); });
  await vm.runInContext('testEmailConnection()', context);
  assert.ok(nodes.emailTestResult.innerHTML.includes('&lt;b&gt;network&lt;/b&gt;'));
});

test('websocket INIT restores batch controls after a page refresh', () => {
  const { context, nodes } = page();
  vm.runInContext('populateConfigForm = () => {}; renderTasks = () => {}; updateStats = () => {}; handleWsMessage({type:"INIT", tasks:[], config:{}, running:true, concurrency:2})', context);
  assert.ok(nodes.batchStatusBadge.innerHTML.includes('2 路并发运行中'));
});

for (const detail of ['任务正在执行', [{ msg: '并发数超出范围' }]]) {
  test(`task actions display API errors: ${JSON.stringify(detail)}`, async () => {
    const { context } = page(async () => ({ ok: false, json: async () => ({ detail }) }));
    vm.runInContext('globalThis.notices = []; showToast = (text, level) => notices.push({text, level})', context);
    await vm.runInContext('retryTask("fixture")', context);
    const notices = vm.runInContext('notices', context);
    assert.equal(notices.length, 1);
    assert.equal(notices[0].level, 'error');
    assert.ok(notices[0].text.length > 0);
  });
}
