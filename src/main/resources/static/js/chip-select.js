/**
 * Chip Select Component
 * A reusable inline chip-style multi-select with search dropdown.
 * 
 * Features:
 * - Shows selected items as removable chips inline
 * - Type to search and filter dropdown options
 * - Fetches options from an API endpoint (supports async search)
 * - Can also use static options
 * - Form-friendly: creates hidden inputs for each selected value
 * - Supports gender-based chip coloring (genderId: 1=female/pink, 2=male/blue)
 * 
 * Usage:
 *   initChipSelect({
 *     containerId: 'artistChipSelect',
 *     inputName: 'artist',               // Name for hidden inputs (for form submission)
 *     placeholder: 'Type to search...',
 *     searchUrl: '/api/artists/search',  // API endpoint for searching
 *     searchParam: 'q',                  // Query parameter name
 *     valueField: 'artistId',            // Field name for the value in API response
 *     labelField: 'artistName',          // Field name for the display label
 *     genderField: 'genderId',           // Optional: Field name for gender (1=female, 2=male)
 *     minChars: 2,                        // Min characters before searching
 *     initialValues: [{value: 1, label: 'Artist Name', genderId: 2}]  // Pre-selected values
 *   });
 */

class ChipSelect {
    constructor(options) {
        this.containerId = options.containerId;
        this.inputName = options.inputName;
        this.placeholder = options.placeholder || 'Type to search...';
        this.searchUrl = options.searchUrl;
        this.searchParam = options.searchParam || 'q';
        this.valueField = options.valueField || 'id';
        this.labelField = options.labelField || 'name';
        this.genderField = options.genderField || null; // Optional: for gender-based coloring
        this.minChars = options.minChars || 2;
        this.initialValues = options.initialValues || [];
        this.debounceMs = options.debounceMs || 500;
        this.staticOptions = options.staticOptions || null; // If provided, use static options instead of API
        
        this.container = document.getElementById(this.containerId);
        if (!this.container) {
            console.error(`ChipSelect: Container with id "${this.containerId}" not found`);
            return;
        }
        
        this.selectedItems = new Map(); // Map of value -> {label, genderId}
        this.debounceTimer = null;
        this.isOpen = false;
        this.lastSearchQuery = ''; // Track the last query sent to prevent race conditions
        
        this.init();
    }
    
    init() {
        // Build the component structure
        this.container.classList.add('chip-select-container');
        this.container.innerHTML = `
            <div class="chip-select-display">
                <div class="chip-select-chips"></div>
                <input type="text" class="chip-select-input" placeholder="${this.placeholder}" autocomplete="off">
                <button type="button" class="chip-select-clear" title="Clear all" style="display: none;">×</button>
            </div>
            <div class="chip-select-dropdown"></div>
            <div class="chip-select-hidden-inputs"></div>
        `;
        
        this.displayEl = this.container.querySelector('.chip-select-display');
        this.chipsEl = this.container.querySelector('.chip-select-chips');
        this.inputEl = this.container.querySelector('.chip-select-input');
        this.dropdownEl = this.container.querySelector('.chip-select-dropdown');
        this.hiddenInputsEl = this.container.querySelector('.chip-select-hidden-inputs');
        this.clearBtn = this.container.querySelector('.chip-select-clear');
        
        // Add initial values
        this.initialValues.forEach(item => {
            this.addChip(item.value, item.label, item.genderId);
        });
        
        this.bindEvents();
        this.updatePlaceholder();
    }
    
    bindEvents() {
        // Focus on input when clicking the display area
        this.displayEl.addEventListener('click', (e) => {
            if (e.target.closest('.chip-select-chip-remove')) return;
            this.inputEl.focus();
        });
        
        // Handle input for search
        this.inputEl.addEventListener('input', () => {
            const query = this.inputEl.value.trim();
            
            if (this.debounceTimer) {
                clearTimeout(this.debounceTimer);
            }
            
            if (query.length < this.minChars) {
                this.closeDropdown();
                return;
            }
            
            this.debounceTimer = setTimeout(() => {
                // Store what we're searching for to compare later
                this.lastSearchQuery = query;
                this.search(query);
            }, this.debounceMs);
        });
        
        // Handle keyboard navigation
        this.inputEl.addEventListener('keydown', (e) => {
            if (e.key === 'Backspace' && this.inputEl.value === '' && this.selectedItems.size > 0) {
                // Remove last chip
                const lastKey = Array.from(this.selectedItems.keys()).pop();
                if (lastKey !== undefined) {
                    this.removeChip(lastKey);
                }
            } else if (e.key === 'Escape') {
                this.closeDropdown();
                this.inputEl.blur();
            } else if (e.key === 'ArrowDown' && this.isOpen) {
                e.preventDefault();
                this.navigateDropdown(1);
            } else if (e.key === 'ArrowUp' && this.isOpen) {
                e.preventDefault();
                this.navigateDropdown(-1);
            } else if (e.key === 'Enter' && this.isOpen) {
                e.preventDefault();
                const highlighted = this.dropdownEl.querySelector('.chip-select-option.highlighted');
                if (highlighted) {
                    highlighted.click();
                }
            }
        });
        
        // Clear button
        this.clearBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            this.clear();
        });
        
        // Close dropdown on outside click
        document.addEventListener('click', (e) => {
            if (!this.container.contains(e.target)) {
                this.closeDropdown();
            }
        });
    }
    
    async search(query) {
        if (this.staticOptions) {
            // Filter static options
            const filtered = this.staticOptions.filter(opt => 
                opt[this.labelField].toLowerCase().includes(query.toLowerCase()) &&
                !this.selectedItems.has(String(opt[this.valueField]))
            );
            // Only update if this search is still relevant (user hasn't typed more)
            if (this.inputEl.value.trim() === query) {
                this.renderDropdown(filtered);
            }
        } else {
            // Fetch from API
            try {
                const url = `${this.searchUrl}?${this.searchParam}=${encodeURIComponent(query)}`;
                const response = await fetch(url);
                if (!response.ok) throw new Error('Search failed');
                const results = await response.json();
                
                // Only update dropdown if user hasn't typed more since this search started
                const currentInput = this.inputEl.value.trim();
                if (currentInput !== query) {
                    // User has typed more, ignore these results
                    return;
                }
                
                // Filter out already selected items
                const filtered = results.filter(item => 
                    !this.selectedItems.has(String(item[this.valueField]))
                );
                
                this.renderDropdown(filtered);
            } catch (error) {
                console.error('ChipSelect search error:', error);
                // Only show error if search is still relevant
                if (this.inputEl.value.trim() === query) {
                    this.dropdownEl.innerHTML = '<div class="chip-select-no-results">Search failed</div>';
                    this.openDropdown();
                }
            }
        }
    }
    
    renderDropdown(items) {
        if (items.length === 0) {
            this.dropdownEl.innerHTML = '<div class="chip-select-no-results">No results found</div>';
            this.openDropdown();
            return;
        }
        
        this.dropdownEl.innerHTML = '';
        
        items.forEach((item, index) => {
            const value = item[this.valueField];
            const label = item[this.labelField];
            const genderId = this.genderField ? item[this.genderField] : null;
            
            const optionEl = document.createElement('div');
            optionEl.className = 'chip-select-option';
            if (index === 0) optionEl.classList.add('highlighted');
            optionEl.dataset.value = value;
            optionEl.dataset.label = label;
            if (genderId) optionEl.dataset.genderId = genderId;
            optionEl.textContent = label;
            
            optionEl.addEventListener('click', () => {
                this.addChip(value, label, genderId);
                this.inputEl.value = '';
                this.closeDropdown();
                this.inputEl.focus();
            });
            
            optionEl.addEventListener('mouseenter', () => {
                this.dropdownEl.querySelectorAll('.chip-select-option').forEach(el => el.classList.remove('highlighted'));
                optionEl.classList.add('highlighted');
            });
            
            this.dropdownEl.appendChild(optionEl);
        });
        
        this.openDropdown();
    }
    
    navigateDropdown(direction) {
        const options = this.dropdownEl.querySelectorAll('.chip-select-option');
        if (options.length === 0) return;
        
        let currentIndex = -1;
        options.forEach((opt, i) => {
            if (opt.classList.contains('highlighted')) {
                currentIndex = i;
            }
        });
        
        let newIndex = currentIndex + direction;
        if (newIndex < 0) newIndex = options.length - 1;
        if (newIndex >= options.length) newIndex = 0;
        
        options.forEach(opt => opt.classList.remove('highlighted'));
        options[newIndex].classList.add('highlighted');
        options[newIndex].scrollIntoView({ block: 'nearest' });
    }
    
    openDropdown() {
        this.dropdownEl.style.display = 'block';
        this.isOpen = true;
    }
    
    closeDropdown() {
        this.dropdownEl.style.display = 'none';
        this.isOpen = false;
    }
    
    addChip(value, label, genderId = null) {
        const valueStr = String(value);
        if (this.selectedItems.has(valueStr)) return;
        
        this.selectedItems.set(valueStr, { label, genderId });
        
        // Create chip element
        const chipEl = document.createElement('span');
        chipEl.className = 'chip-select-chip';
        
        // Apply gender-based styling
        if (genderId === 1) {
            chipEl.classList.add('chip-gender-female');
        } else if (genderId === 2) {
            chipEl.classList.add('chip-gender-male');
        }
        
        chipEl.dataset.value = valueStr;
        if (genderId) chipEl.dataset.genderId = genderId;
        chipEl.innerHTML = `
            <span class="chip-select-chip-label">${this.escapeHtml(label)}</span>
            <button type="button" class="chip-select-chip-remove" aria-label="Remove">×</button>
        `;
        
        chipEl.querySelector('.chip-select-chip-remove').addEventListener('click', (e) => {
            e.stopPropagation();
            this.removeChip(valueStr);
        });
        
        this.chipsEl.appendChild(chipEl);
        
        // Create hidden input for form submission
        const hiddenInput = document.createElement('input');
        hiddenInput.type = 'hidden';
        hiddenInput.name = this.inputName;
        hiddenInput.value = valueStr; // Use ID for exact matching
        hiddenInput.dataset.chipValue = valueStr;
        this.hiddenInputsEl.appendChild(hiddenInput);
        
        this.updatePlaceholder();
    }
    
    removeChip(value) {
        const valueStr = String(value);
        this.selectedItems.delete(valueStr);
        
        // Remove chip element
        const chipEl = this.chipsEl.querySelector(`[data-value="${CSS.escape(valueStr)}"]`);
        if (chipEl) chipEl.remove();
        
        // Remove hidden input
        const hiddenInput = this.hiddenInputsEl.querySelector(`[data-chip-value="${CSS.escape(valueStr)}"]`);
        if (hiddenInput) hiddenInput.remove();
        
        this.updatePlaceholder();
    }
    
    updatePlaceholder() {
        if (this.selectedItems.size > 0) {
            this.inputEl.placeholder = '';
            this.clearBtn.style.display = '';
        } else {
            this.inputEl.placeholder = this.placeholder;
            this.clearBtn.style.display = 'none';
        }
    }
    
    getSelectedValues() {
        return Array.from(this.selectedItems.keys());
    }
    
    getSelectedLabels() {
        return Array.from(this.selectedItems.values());
    }
    
    clear() {
        this.selectedItems.clear();
        this.chipsEl.innerHTML = '';
        this.hiddenInputsEl.innerHTML = '';
        this.updatePlaceholder();
    }
    
    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

/**
 * Factory function for easier initialization
 */
function initChipSelect(options) {
    return new ChipSelect(options);
}
