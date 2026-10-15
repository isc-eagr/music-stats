const pendingSongConfirmations = new Map();

function cancelSongCreationConfirmation(modalId) {
    pendingSongConfirmations.get(modalId)?.cancel();
}

async function confirmSongCreation(artistId, name, modalId) {
    // A newly created artist cannot have existing songs.
    if (!artistId) return true;

    cancelSongCreationConfirmation(modalId);
    const modal = document.getElementById(modalId);
    const panel = modal.querySelector('[data-song-duplicate-confirmation]');
    const confirmBtn = panel.querySelector('[data-confirm-duplicate]');
    const cancelBtn = panel.querySelector('[data-cancel-duplicate]');
    let cancelled = false;
    const state = { cancel: () => { cancelled = true; } };
    pendingSongConfirmations.set(modalId, state);
    const cancel = () => state.cancel();
    modal.addEventListener('input', cancel);
    modal.addEventListener('change', cancel);

    try {
        const params = new URLSearchParams({ artistId, name });
        const response = await fetch('/songs/api/duplicate?' + params, { cache: 'no-store' });
        if (cancelled) return false;
        if (!response.ok) throw new Error('Unable to check for duplicate songs');

        const data = await response.json();
        if (cancelled) return false;
        if (typeof data.exists !== 'boolean') throw new Error('Invalid duplicate song response');
        if (!data.exists) return true;

        return await new Promise(resolve => {
            state.cancel = () => resolve(false);
            confirmBtn.onclick = () => resolve(true);
            cancelBtn.onclick = () => resolve(false);
            panel.hidden = false;
            panel.scrollIntoView({ block: 'nearest' });
            panel.focus();
        });
    } catch (error) {
        if (cancelled) return false;
        throw error;
    } finally {
        modal.removeEventListener('input', cancel);
        modal.removeEventListener('change', cancel);
        if (pendingSongConfirmations.get(modalId) === state) {
            panel.hidden = true;
            confirmBtn.onclick = null;
            cancelBtn.onclick = null;
            pendingSongConfirmations.delete(modalId);
        }
    }
}
