const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const resources = path.join(__dirname, '../../main/resources');
const helper = fs.readFileSync(path.join(resources, 'static/js/song-creation.js'), 'utf8');
const list = fs.readFileSync(path.join(resources, 'templates/songs/list.html'), 'utf8');
const wizard = fs.readFileSync(path.join(resources, 'templates/fragments/unmatched-plays-modal.html'), 'utf8');
const confirmation = fs.readFileSync(path.join(resources, 'templates/fragments/song-creation-confirmation.html'), 'utf8');
const flush = () => new Promise(resolve => setImmediate(resolve));

function setup(source, name, { exists = true, confirmed = false, failed = false, newArtist = false } = {}) {
    const requests = [], prompts = [], errors = [];
    const confirmButton = {}, cancelButton = {};
    const panel = {
        hidden: true,
        querySelector: selector => selector === '[data-confirm-duplicate]' ? confirmButton : cancelButton,
        scrollIntoView() {},
        focus() { prompts.push(confirmation.match(/<p role="status">(.*?)<\/p>/)[1]); }
    };
    const fields = new Map();
    const values = { wizardSongNameInput: "Don't Stop & Go", wizardSongReleaseDate: '14/10/2026',
        wizardSongLength: '3:45', wizardSongTrackNumber: '', wizardCreateBtn: '' };
    const get = id => {
        if (!fields.has(id)) fields.set(id, {
            value: values[id] || '', disabled: false, textContent: 'Create', checked: false,
            listeners: {}, querySelector: () => panel,
            addEventListener(event, fn) { this.listeners[event] = fn; },
            removeEventListener(event, fn) { if (this.listeners[event] === fn) delete this.listeners[event]; }
        });
        return fields.get(id);
    };
    const button = get('wizardCreateBtn');
    const context = {
        URLSearchParams, console,
        window: { confirm() { assert.fail('must show confirmation inside the modal'); }, location: {} },
        document: { getElementById: get },
        FormData: class {
            forEach(fn) { Object.entries({ name: "Don't Stop & Go", artistId: '1' }).forEach(([key, value]) => fn(value, key)); }
        },
        showWarningToast: message => errors.push(message),
        showErrorToast: message => errors.push(message), showSuccessToast() {},
        refreshUnmatchedList() {}, closeCreateWizard() {}, createSongPendingImageUrl: null,
        wizardState: { selectedArtistId: 1, isNewArtist: newArtist, playData: { artist: 'Artist', song: 'Song' } },
        fetch: async (url, options) => {
            requests.push({ url, options });
            if (url.startsWith('/songs/api/duplicate')) {
                return { ok: !failed, json: async () => ({ exists }) };
            }
            return { ok: true, json: async () => ({ id: 7, success: true, updatedCount: 1 }) };
        }
    };
    vm.createContext(context);
    vm.runInContext(helper, context);
    const start = source.indexOf(`async function ${name}(`);
    const end = source.indexOf('\n        }', start) + '\n        }'.length;
    assert.ok(start >= 0 && end > start, 'creation handler exists');
    vm.runInContext(source.slice(start, end), context);
    const begin = () => context[name]({ preventDefault() {}, target: { querySelector: () => button } });
    return { requests, prompts, errors, button, panel, start: begin,
        cancel: () => context.cancelSongCreationConfirmation(name === 'submitCreateSong' ? 'createSongModal' : 'createWizardModal'),
        edit: event => get(name === 'submitCreateSong' ? 'createSongModal' : 'createWizardModal').listeners[event]?.(),
        async submit() {
            const pending = begin();
            await flush();
            if (!panel.hidden) (confirmed ? confirmButton : cancelButton).onclick();
            await pending;
            await flush();
        }
    };
}

for (const [label, source, name, endpoint] of [
    ['New Song', list, 'submitCreateSong', '/songs/create'],
    ['Create Wizard', wizard, 'createAndAssignSong', '/plays/api/create-and-assign']
]) {
    test(`${label}: cancelling a duplicate keeps the form and prevents saving`, async () => {
        const ui = setup(source, name);
        await ui.submit();
        assert.equal(ui.requests.length, 1);
        assert.equal(ui.prompts.length, 1);
        assert.match(ui.prompts[0], /exact same title by the same artist/);
        assert.equal(ui.button.disabled, false);
        assert.equal(ui.panel.hidden, true);
        const params = new URL(ui.requests[0].url, 'http://localhost').searchParams;
        assert.equal(params.get('artistId'), '1');
        assert.equal(params.get('name'), "Don't Stop & Go");
    });

    test(`${label}: confirming a duplicate saves`, async () => {
        const ui = setup(source, name, { confirmed: true });
        await ui.submit();
        assert.equal(ui.prompts.length, 1);
        assert.equal(ui.requests[1].url, endpoint);
        assert.equal(ui.requests[1].options.method, 'POST');
    });

    test(`${label}: a unique title saves without prompting`, async () => {
        const ui = setup(source, name, { exists: false });
        await ui.submit();
        assert.equal(ui.prompts.length, 0);
        assert.equal(ui.requests[1].url, endpoint);
    });

    test(`${label}: a failed check prevents saving and allows retry`, async () => {
        const ui = setup(source, name, { failed: true });
        await ui.submit();
        assert.equal(ui.requests.length, 1);
        assert.equal(ui.prompts.length, 0);
        assert.match(ui.errors[0], /Unable to check for duplicate songs/);
        assert.equal(ui.button.disabled, false);
    });

    test(`${label}: ignores repeated clicks while checking`, async () => {
        const ui = setup(source, name);
        await Promise.all([ui.submit(), ui.submit()]);
        assert.equal(ui.requests.length, 1);
    });

    test(`${label}: waits for a decision inside the modal before saving`, async () => {
        const ui = setup(source, name);
        const pending = ui.start();
        await flush();
        assert.equal(ui.panel.hidden, false);
        assert.equal(ui.button.disabled, true);
        assert.equal(ui.requests.length, 1);
        ui.cancel();
        await pending;
        assert.equal(ui.panel.hidden, true);
        assert.equal(ui.button.disabled, false);
        assert.equal(ui.requests.length, 1);
    });

    for (const event of ['input', 'change']) {
        test(`${label}: editing the form dismisses the pending confirmation (${event})`, async () => {
            const ui = setup(source, name);
            const pending = ui.start();
            await flush();
            ui.edit(event);
            await pending;
            assert.equal(ui.panel.hidden, true);
            assert.equal(ui.button.disabled, false);
            assert.equal(ui.requests.length, 1);
        });
    }
}

test('Create Wizard: a new artist has no songs to check', async () => {
    const ui = setup(wizard, 'createAndAssignSong', { newArtist: true });
    await ui.submit();
    assert.equal(ui.prompts.length, 0);
    assert.equal(ui.requests.length, 1);
    assert.equal(ui.requests[0].url, '/plays/api/create-and-assign');
});

test('both creation surfaces load the shared confirmation script', () => {
    assert.match(list, /<script th:src="@\{\/js\/song-creation\.js\}"><\/script>/);
    assert.match(wizard, /<th:block th:fragment="modal-scripts">\s*<script th:src="@\{\/js\/song-creation\.js\}"><\/script>/);
    for (const template of [list, wizard]) {
        assert.match(template, /fragments\/song-creation-confirmation :: confirmation/);
        assert.match(template, /@\{\/css\/song-creation\.css\}/);
    }
    assert.match(confirmation, /hidden tabindex="-1" role="group"/);
    assert.match(confirmation, /type="button"[^>]*data-cancel-duplicate>Go back/);
    assert.match(confirmation, /type="button"[^>]*data-confirm-duplicate>Create anyway/);
});
