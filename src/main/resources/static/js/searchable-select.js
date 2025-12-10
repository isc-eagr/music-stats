/**
 * Searchable Select Component
 * Converts a standard <select> into a searchable dropdown
 * 
 * Usage: 
 *   initSearchableSelect('selectId')
 *   or add class="searchable-select" to the select element
 */

function initSearchableSelect(selectId) {
    const select = document.getElementById(selectId);
    if (!select || select.dataset.searchableInitialized) return;
    
    select.dataset.searchableInitialized = 'true';
    
    // Create wrapper
    const wrapper = document.createElement('div');
    wrapper.className = 'searchable-select-wrapper';
    select.parentNode.insertBefore(wrapper, select);
    
    // Create display element
    const display = document.createElement('div');
    display.className = 'searchable-select-display';
    
    // Create input for searching
    const input = document.createElement('input');
    input.type = 'text';
    input.className = 'searchable-select-input';
    input.placeholder = 'Type to search...';
    input.autocomplete = 'off';
    
    // Create dropdown
    const dropdown = document.createElement('div');
    dropdown.className = 'searchable-select-dropdown';
    
    // Move select into wrapper (hidden)
    wrapper.appendChild(display);
    wrapper.appendChild(input);
    wrapper.appendChild(dropdown);
    wrapper.appendChild(select);
    select.style.display = 'none';
    
    // Build options list
    function buildOptions(filter = '') {
        dropdown.innerHTML = '';
        const filterLower = filter.toLowerCase();
        
        Array.from(select.options).forEach((option, index) => {
            if (filter && !option.text.toLowerCase().includes(filterLower)) {
                return;
            }
            
            const optionDiv = document.createElement('div');
            optionDiv.className = 'searchable-select-option';
            // Mark selected option either by the option.selected flag or by value match
            if (option.selected || option.value === select.value) {
                optionDiv.classList.add('selected');
            }
            optionDiv.textContent = option.text;
            optionDiv.dataset.value = option.value;
            optionDiv.dataset.index = index;
            
            optionDiv.addEventListener('click', function() {
                selectOption(option.value, option.text);
            });
            
            dropdown.appendChild(optionDiv);
        });
        
        if (dropdown.children.length === 0) {
            const noResults = document.createElement('div');
            noResults.className = 'searchable-select-no-results';
            noResults.textContent = 'No results found';
            dropdown.appendChild(noResults);
        }
    }
    
    function selectOption(value, text) {
        select.value = value;
        display.textContent = text || 'Select...';
        display.classList.toggle('has-value', !!value);
        input.value = '';
        closeDropdown();
        
        // Trigger change event
        select.dispatchEvent(new Event('change', { bubbles: true }));
    }
    
    function updateDisplay() {
        const selectedOption = select.options[select.selectedIndex];
        if (selectedOption) {
            // Always display the selected option's text (even if its value is empty)
            display.textContent = selectedOption.text;
            display.classList.add('has-value');
        } else {
            display.textContent = 'Select...';
            display.classList.remove('has-value');
        }
    }
    
    function openDropdown() {
        wrapper.classList.add('open');
        input.style.display = 'block';
        dropdown.style.display = 'block';
        buildOptions();
        input.focus();
    }
    
    function closeDropdown() {
        wrapper.classList.remove('open');
        input.style.display = 'none';
        dropdown.style.display = 'none';
        input.value = '';
    }
    
    function isOpen() {
        return wrapper.classList.contains('open');
    }
    
    // Event listeners
    display.addEventListener('click', function(e) {
        e.stopPropagation();
        if (isOpen()) {
            closeDropdown();
        } else {
            openDropdown();
        }
    });
    
    input.addEventListener('input', function() {
        buildOptions(this.value);
    });
    
    input.addEventListener('keydown', function(e) {
        if (e.key === 'Escape') {
            closeDropdown();
        } else if (e.key === 'Enter') {
            const firstOption = dropdown.querySelector('.searchable-select-option');
            if (firstOption) {
                selectOption(firstOption.dataset.value, firstOption.textContent);
            }
        } else if (e.key === 'ArrowDown') {
            e.preventDefault();
            const options = dropdown.querySelectorAll('.searchable-select-option');
            const current = dropdown.querySelector('.searchable-select-option.highlighted');
            let next = current ? current.nextElementSibling : options[0];
            if (next && next.classList.contains('searchable-select-option')) {
                if (current) current.classList.remove('highlighted');
                next.classList.add('highlighted');
                next.scrollIntoView({ block: 'nearest' });
            }
        } else if (e.key === 'ArrowUp') {
            e.preventDefault();
            const current = dropdown.querySelector('.searchable-select-option.highlighted');
            if (current && current.previousElementSibling) {
                current.classList.remove('highlighted');
                current.previousElementSibling.classList.add('highlighted');
                current.previousElementSibling.scrollIntoView({ block: 'nearest' });
            }
        }
    });
    
    // Close on outside click
    document.addEventListener('click', function(e) {
        if (!wrapper.contains(e.target)) {
            closeDropdown();
        }
    });
    
    // Initialize display
    updateDisplay();
    
    // Watch for programmatic changes to select
    const observer = new MutationObserver(function() {
        updateDisplay();
    });
    observer.observe(select, { attributes: true, childList: true, subtree: true });
    
    return {
        open: openDropdown,
        close: closeDropdown,
        refresh: updateDisplay
    };
}

// Auto-initialize all searchable selects when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    document.querySelectorAll('select.searchable-select').forEach(function(select) {
        if (select.id) {
            initSearchableSelect(select.id);
        }
    });
});

// Initialize searchable selects when entering edit mode
function initAllSearchableSelects(container) {
    const selects = container ? container.querySelectorAll('select') : document.querySelectorAll('select');
    selects.forEach(function(select) {
        if (select.id && !select.dataset.searchableInitialized) {
            // Only init selects that are visible and have more than a few options
            if (select.options.length > 5) {
                initSearchableSelect(select.id);
            }
        }
    });
}
