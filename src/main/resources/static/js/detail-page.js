/* Shared helpers of the artist, album and song detail pages (profile edit mode, form snapshot/restore,
   tag chips, gallery navigation, field highlights). Loaded before the page scripts that call them. */
function cancelEdit() {
    exitEditMode();
}

function captureFormState(formId) {
    const form = document.getElementById(formId);
    if (!form) {
        return null;
    }

    const values = {};
    const selectOptions = {};

    Array.from(form.elements).forEach(element => {
        if (!element.name) {
            return;
        }

        if (element.tagName === 'SELECT' && element.id) {
            selectOptions[element.id] = element.innerHTML;
        }

        if (element.type === 'checkbox' || element.type === 'radio') {
            if (!values[element.name]) {
                values[element.name] = [];
            }
            if (element.checked) {
                values[element.name].push(element.value);
            }
            return;
        }

        if (element.tagName === 'SELECT' && element.multiple) {
            values[element.name] = Array.from(element.selectedOptions).map(option => option.value);
            return;
        }

        values[element.name] = element.value;
    });

    return { values, selectOptions };
}

function clearAllHighlights() {
    document.querySelectorAll('.field-highlight').forEach(el => el.classList.remove('field-highlight'));
}

function cloneStateArray(items) {
    return Array.isArray(items) ? items.map(item => ({ ...item })) : [];
}

function closeProfileMenu() {
    const menu = document.getElementById('profileMenu');
    const button = document.getElementById('profileMenuBtn');
    if (menu && !menu.hidden) {
        menu.hidden = true;
        if (button) {
            button.setAttribute('aria-expanded', 'false');
        }
    }
}

function destroyFlatpickrInstance(inputId) {
    const input = document.getElementById(inputId);
    if (input && input._flatpickr) {
        input._flatpickr.destroy();
    }
}

function destroySearchableSelects(formId) {
    const form = document.getElementById(formId);
    if (!form) {
        return;
    }

    form.querySelectorAll('select[data-searchable-initialized]').forEach(select => {
        const wrapper = select.closest('.searchable-select-wrapper');
        if (wrapper && wrapper.parentNode) {
            wrapper.parentNode.insertBefore(select, wrapper);
            wrapper.remove();
        }
        delete select.dataset.searchableInitialized;
        select.style.display = 'none';
    });
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function galleryNavigate(direction) {
    let newIndex = currentGalleryIndex + direction;
    // Wrap around carousel
    if (newIndex < 0) newIndex = galleryImages.length - 1;
    if (newIndex >= galleryImages.length) newIndex = 0;
    currentGalleryIndex = newIndex;
    updateGalleryImage();
    updateGalleryUI();
}

// External Metadata Search for Album Edit Mode
// Marks the field box a fetched value landed in, until the user touches it
function highlightField(fieldId) {
    const field = document.getElementById(fieldId);
    const box = field ? field.closest('.dt-fb') : null;
    if (!box) {
        return;
    }
    box.classList.remove('field-highlight');
    void box.offsetWidth; // restart the highlight
    box.classList.add('field-highlight');
    const removeHighlight = () => {
        box.classList.remove('field-highlight');
        box.removeEventListener('input', removeHighlight);
        box.removeEventListener('change', removeHighlight);
    };
    box.addEventListener('input', removeHighlight);
    box.addEventListener('change', removeHighlight);
    updateProfileDirtyState();
}

function initializeTagChipSelect() {
    tagChipSelect = initChipSelect({
        containerId: 'tagChipSelect',
        inputName: 'tags',
        placeholder: 'Search tags...',
        staticOptions: tagOptions || [],
        valueField: 'id',
        labelField: 'name',
        minChars: 0,
        debounceMs: 100,
        showAllOnFocus: true,
        initialValues: toTagChipValues(initialAssignedTags)
    });
}

function resetTagChipSelect() {
    if (!tagChipSelect) {
        return;
    }
    tagChipSelect.clear();
    toTagChipValues(initialAssignedTags).forEach(tag => tagChipSelect.addChip(tag.value, tag.label));
}

function restoreFormState(formId, state) {
    const form = document.getElementById(formId);
    if (!form || !state) {
        return;
    }

    form.querySelectorAll('select[id]').forEach(select => {
        if (state.selectOptions[select.id] !== undefined) {
            select.innerHTML = state.selectOptions[select.id];
        }
    });

    Array.from(form.elements).forEach(element => {
        if (!element.name) {
            return;
        }

        const savedValue = state.values[element.name];

        if (element.type === 'checkbox' || element.type === 'radio') {
            const selectedValues = Array.isArray(savedValue) ? savedValue : [];
            element.checked = selectedValues.includes(element.value);
            return;
        }

        if (element.tagName === 'SELECT' && element.multiple) {
            const selectedValues = Array.isArray(savedValue) ? savedValue : [];
            Array.from(element.options).forEach(option => {
                option.selected = selectedValues.includes(option.value);
            });
            return;
        }

        element.value = savedValue ?? '';
    });
}

function showDetailSaveOverlay() {
    document.body.classList.add('detail-save-busy');
}

function toTagChipValues(tags) {
    return Array.isArray(tags) ? tags.map(tag => ({ value: tag.id, label: tag.name })) : [];
}

function toggleEdit() {
    if (isEditing) {
        exitEditMode();
    } else {
        enterEditMode();
    }
}

// Overflow menu next to Edit (Fetch external data, Delete)
function toggleProfileMenu(event) {
    event.stopPropagation();
    const menu = document.getElementById('profileMenu');
    const button = document.getElementById('profileMenuBtn');
    if (!menu || !button) {
        return;
    }
    const open = menu.hidden;
    menu.hidden = !open;
    button.setAttribute('aria-expanded', String(open));
}

function updateGalleryUI() {
    const prevBtn = document.getElementById('galleryPrev');
    const nextBtn = document.getElementById('galleryNext');
    const counter = document.getElementById('galleryCounter');
    const currentIndexSpan = document.getElementById('currentImageIndex');
    const totalCountSpan = document.getElementById('totalImageCount');
    const btnSetDefault = document.getElementById('btnSetDefault');
    const btnRemoveGallery = document.getElementById('btnRemoveGallery');
    const btnRemoveMain = document.getElementById('btnRemoveMain');
    const btnUploadMain = document.getElementById('btnUploadImage');

    const hasMultipleImages = galleryImages.length > 1;
    const isOnSecondaryImage = galleryImages[currentGalleryIndex] !== 0;

    if (prevBtn) prevBtn.style.display = hasMultipleImages ? 'flex' : 'none';
    if (nextBtn) nextBtn.style.display = hasMultipleImages ? 'flex' : 'none';
    if (counter) counter.style.display = hasMultipleImages ? 'block' : 'none';
    if (currentIndexSpan) currentIndexSpan.textContent = currentGalleryIndex + 1;
    if (totalCountSpan) totalCountSpan.textContent = galleryImages.length;

    // Upload button is always visible
    if (btnUploadMain) btnUploadMain.style.display = 'inline-block';

    if (isOnSecondaryImage) {
        if (btnSetDefault) btnSetDefault.style.display = 'inline-block';
        if (btnRemoveGallery) btnRemoveGallery.style.display = 'inline-block';
        if (btnRemoveMain) btnRemoveMain.style.display = 'none';
    } else {
        if (btnSetDefault) btnSetDefault.style.display = 'none';
        if (btnRemoveGallery) btnRemoveGallery.style.display = 'none';
        // Only show remove button if this is the only image
        if (btnRemoveMain) btnRemoveMain.style.display = hasMultipleImages ? 'none' : 'inline-block';
    }
}
