(function(){
  /**
   * Shared table sorting utility for Music Stats
   * Handles date, time, number, and string sorting with proper parsing
   */

  const monthMap = {
    'jan': 0, 'feb': 1, 'mar': 2, 'apr': 3, 'may': 4, 'jun': 5,
    'jul': 6, 'aug': 7, 'sep': 8, 'oct': 9, 'nov': 10, 'dec': 11
  };

  function parseTimeToSeconds(txt){
    // Supports mm:ss or hh:mm:ss or Xd Yh or Xh Ym formats
    const parts = txt.split(':').map(p=>p.trim());
    if(parts.length === 2){
      const [m,s] = parts.map(Number);
      if(!Number.isNaN(m) && !Number.isNaN(s)) return m*60 + s;
    }
    if(parts.length === 3){
      const [h,m,s] = parts.map(Number);
      if(!Number.isNaN(h) && !Number.isNaN(m) && !Number.isNaN(s)) return h*3600 + m*60 + s;
    }
    // Handle formats like "2d 5h" or "3h 15m"
    const dayHour = txt.match(/(\d+)d\s*(\d+)h/i);
    if(dayHour){
      return parseInt(dayHour[1]) * 86400 + parseInt(dayHour[2]) * 3600;
    }
    const hourMin = txt.match(/(\d+)h\s*(\d+)m/i);
    if(hourMin){
      return parseInt(hourMin[1]) * 3600 + parseInt(hourMin[2]) * 60;
    }
    const minOnly = txt.match(/^(\d+)m$/i);
    if(minOnly){
      return parseInt(minOnly[1]) * 60;
    }
    return NaN;
  }

  function parseDate(txt){
    // Parse "dd MMM yyyy" format (e.g., "21 Nov 2025" or "1 Feb 2012")
    // Also handles "dd-MMM-yyyy" format (e.g., "01-Nov-2025" or "1-Feb-2012")
    const separator = txt.includes('-') ? /-/ : /\s+/;
    const parts = txt.trim().split(separator);
    if(parts.length === 3){
      const day = parseInt(parts[0]);
      const month = monthMap[parts[1].toLowerCase().substring(0, 3)];
      const year = parseInt(parts[2]);
      
      if(!Number.isNaN(day) && month !== undefined && !Number.isNaN(year)){
        return new Date(year, month, day).getTime();
      }
    }
    return NaN;
  }

  function parseCell(text, dataType){
    if(!text) return {type:'string', value:''};
    const t = text.trim();
    if(!t || t === '-') return {type:'string', value:''};

    // If explicit data-type provided, use it
    if(dataType === 'date'){
      const dateVal = parseDate(t);
      if(!Number.isNaN(dateVal)) return {type:'date', value:dateVal};
      return {type:'string', value:''};
    }
    if(dataType === 'number'){
      const num = parseFloat(t.replace(/,/g, ''));
      if(!Number.isNaN(num)) return {type:'number', value:num};
      return {type:'number', value:0};
    }
    if(dataType === 'duration'){
      const secs = parseTimeToSeconds(t);
      if(!Number.isNaN(secs)) return {type:'number', value:secs};
      return {type:'number', value:0};
    }

    // Auto-detect type
    // Date check (dd MMM yyyy)
    const dateVal = parseDate(t);
    if(!Number.isNaN(dateVal)) return {type:'date', value:dateVal};

    // Percent e.g. 12.34%
    if(t.endsWith('%')){
      const num = parseFloat(t.slice(0,-1));
      if(!Number.isNaN(num)) return {type:'number', value:num};
    }

    // Time like mm:ss or hh:mm:ss or Xd Yh
    if(t.includes(':') || /\d+[dhm]/i.test(t)){
      const secs = parseTimeToSeconds(t);
      if(!Number.isNaN(secs)) return {type:'number', value:secs};
    }

    // Plain number
    const num = parseFloat(t.replace(/,/g, ''));
    if(!Number.isNaN(num) && /^-?\d*[\.,]?\d*(e[+-]?\d+)?$/i.test(t.replace(/,/g,''))){
      return {type:'number', value:num};
    }

    return {type:'string', value:t.toLowerCase()};
  }

  function getCellValue(cell, dataType){
    // First check data-value attribute (preferred for sorting)
    const dataValue = cell.getAttribute && cell.getAttribute('data-value');
    if(dataValue != null && dataValue !== ''){
      // If data-value exists and we know the type, parse accordingly
      if(dataType === 'number' || dataType === 'duration'){
        const num = parseFloat(dataValue);
        if(!Number.isNaN(num)) return {type:'number', value:num};
      }
      if(dataType === 'date'){
        // data-value for dates might be ISO format or display format
        const dateVal = parseDate(dataValue);
        if(!Number.isNaN(dateVal)) return {type:'date', value:dateVal};
        // Try ISO format
        const isoDate = new Date(dataValue).getTime();
        if(!Number.isNaN(isoDate)) return {type:'date', value:isoDate};
      }
      return parseCell(dataValue, dataType);
    }
    
    // Fallback to text content
    const text = (cell.textContent || '').trim();
    return parseCell(text, dataType);
  }

  function makeSortable(table){
    // Find header cells from the first thead row only.
    let headerCells = Array.from(table.tHead?.rows?.[0]?.cells || []);
    if(headerCells.length === 0){
      const firstRow = table.rows && table.rows[0];
      if(firstRow){
        headerCells = Array.from(firstRow.querySelectorAll('th'));
      }
    }
    if(headerCells.length === 0) return; // nothing to do

    table._sortState = Array.isArray(table._sortState) ? table._sortState : [];

    headerCells.forEach((th, idx)=>{
      // Skip non-sortable columns (those without 'sortable' class if explicitly marked)
      // or columns marked with data-no-sort
      if(th.hasAttribute('data-no-sort')) return;
      
      // Only add click handler if column should be sortable
      const isSortable = th.classList.contains('sortable') || !th.classList.contains('no-sort');
      if(!isSortable) return;

      th.classList.add('sortable');
      th.style.cursor = 'pointer';
      
      th.addEventListener('click', (event)=>{
        sortByColumn(table, idx, th, headerCells, { shiftKey: !!event.shiftKey });
      });
    });

    // Apply default sort if specified
    const defaultSortTh = headerCells.find(th => th.hasAttribute('data-default-sort'));
    if(defaultSortTh){
      const idx = headerCells.indexOf(defaultSortTh);
      const dir = defaultSortTh.getAttribute('data-default-sort') || 'desc';
      defaultSortTh.setAttribute('data-sort-dir', dir === 'desc' ? 'asc' : 'desc'); // will be toggled
      // Hide tbody to prevent visible re-sort flash on page load
      const tbody = table.tBodies && table.tBodies.length ? table.tBodies[0] : null;
      if(tbody) tbody.style.visibility = 'hidden';
      sortByColumn(table, idx, defaultSortTh, headerCells, { shiftKey: false });
      if(tbody) requestAnimationFrame(function(){ tbody.style.visibility = 'visible'; });
    }
  }

  function sortByColumn(table, colIndex, th, allHeaders, options){
    const shiftKey = !!(options && options.shiftKey);
    const currentSorts = Array.isArray(table._sortState) ? [...table._sortState] : [];
    const existingIndex = currentSorts.findIndex(sort => sort.colIndex === colIndex);
    const existingSort = existingIndex >= 0 ? currentSorts[existingIndex] : null;
    const dir = existingSort && existingSort.dir === 'asc' ? 'desc' : 'asc';
    let nextSorts;

    if (shiftKey) {
      if (existingIndex >= 0) {
        nextSorts = [...currentSorts];
        nextSorts[existingIndex] = { colIndex, dir };
      } else if (currentSorts.length < 3) {
        nextSorts = [...currentSorts, { colIndex, dir }];
      } else {
        nextSorts = currentSorts;
      }
    } else {
      nextSorts = [{ colIndex, dir }];
    }

    table._sortState = nextSorts;
    applySortState(table, allHeaders, nextSorts);
  }

  function applySortState(table, allHeaders, nextSorts){
    if(!Array.isArray(allHeaders) || allHeaders.length === 0) return;

    allHeaders.forEach((header, headerIndex)=>{
      header.removeAttribute('data-sort-dir');
      header.removeAttribute('data-sort-priority');
      header.classList.remove('sorted-asc','sorted-desc','sort-asc','sort-desc');
      const activeSortIndex = nextSorts.findIndex(sort => sort.colIndex === headerIndex);
      if(activeSortIndex >= 0){
        const activeSort = nextSorts[activeSortIndex];
        header.setAttribute('data-sort-dir', activeSort.dir);
        header.setAttribute('data-sort-priority', String(activeSortIndex + 1));
        header.classList.add(activeSort.dir === 'asc' ? 'sorted-asc' : 'sorted-desc');
        header.classList.add(activeSort.dir === 'asc' ? 'sort-asc' : 'sort-desc');
      }
    });

    // Collect rows: if there's a tbody, take all its rows; otherwise skip header row
    const tbody = table.tBodies && table.tBodies.length ? table.tBodies[0] : null;
    let rows = Array.from((tbody || table).rows);
    if(!tbody) rows = rows.slice(1);

    // Group rows: each regular row may be followed by detail-row siblings that must move with it
    const rowGroups = []; // [{main: tr, details: [tr, ...]}]
    const specialRows = [];
    let ri = 0;
    while (ri < rows.length) {
      const r = rows[ri];
      if (r.classList.contains('non-sortable-row') || (r.id && r.id.includes('Summary'))) {
        specialRows.push(r);
        ri++;
      } else if (r.classList.contains('detail-row')) {
        // Orphaned detail-row (e.g., before any main row): treat as special
        specialRows.push(r);
        ri++;
      } else {
        const group = { main: r, details: [] };
        ri++;
        while (ri < rows.length && rows[ri].classList.contains('detail-row')) {
          group.details.push(rows[ri]);
          ri++;
        }
        rowGroups.push(group);
      }
    }
    const normalizedSorts = nextSorts.map(sort => {
      const header = allHeaders[sort.colIndex];
      const dataType = header ? (header.getAttribute('data-type') || null) : null;
      return {
        ...sort,
        dataType,
        sampleType: dataType || 'string'
      };
    });

    const sortableGroups = rowGroups.map(group => {
      const parsedByColumn = new Map();
      normalizedSorts.forEach(sort => {
        parsedByColumn.set(sort.colIndex, getCellValue(group.main.cells[sort.colIndex] || {}, sort.dataType));
      });
      return { group, parsedByColumn };
    });

    normalizedSorts.forEach(sort => {
      if(sort.dataType){
        return;
      }
      for(const sortableGroup of sortableGroups){
        const parsed = sortableGroup.parsedByColumn.get(sort.colIndex);
        if(parsed && parsed.value !== '' && parsed.type !== 'string'){
          sort.sampleType = parsed.type;
          break;
        }
      }
    });

    sortableGroups.sort((a,b)=>{
      for(const sort of normalizedSorts){
        const pa = a.parsedByColumn.get(sort.colIndex) || { type: 'string', value: '' };
        const pb = b.parsedByColumn.get(sort.colIndex) || { type: 'string', value: '' };

        const aEmpty = pa.value === '' || pa.value == null;
        const bEmpty = pb.value === '' || pb.value == null;
        if(aEmpty && !bEmpty) return 1;
        if(!aEmpty && bEmpty) return -1;

        if(sort.sampleType === 'number' || sort.sampleType === 'date'){
          const va = (pa.type==='number' || pa.type==='date') ? pa.value : Number(pa.value || 0);
          const vb = (pb.type==='number' || pb.type==='date') ? pb.value : Number(pb.value || 0);
          if(va !== vb){
            return sort.dir === 'asc' ? (va - vb) : (vb - va);
          }
          continue;
        }

        const va = (pa.type==='string') ? pa.value : String(pa.value);
        const vb = (pb.type==='string') ? pb.value : String(pb.value);
        const comparison = sort.dir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
        if(comparison !== 0){
          return comparison;
        }
      }
      return 0;
    });

    // Re-append in sorted order using a fragment so large resort operations do not
    // churn the live table body one row at a time.
    const parent = tbody || table;
    const fragment = document.createDocumentFragment();
    sortableGroups.forEach(({ group }) => {
      fragment.appendChild(group.main);
      group.details.forEach(d => fragment.appendChild(d));
    });
    specialRows.forEach(r => fragment.appendChild(r));
    parent.appendChild(fragment);
    
    // Update row numbers after sorting
    updateRowNumbers(table);
  }

  function resortTable(table){
    if(!table) return;
    let headerCells = Array.from(table.tHead?.rows?.[0]?.cells || []);
    if(headerCells.length === 0){
      const firstRow = table.rows && table.rows[0];
      if(firstRow){
        headerCells = Array.from(firstRow.querySelectorAll('th'));
      }
    }
    if(headerCells.length === 0) return;
    const currentSorts = Array.isArray(table._sortState) ? [...table._sortState] : [];
    if(currentSorts.length === 0) return;
    applySortState(table, headerCells, currentSorts);
  }
  
  function updateRowNumbers(table) {
    const tbody = table.tBodies && table.tBodies.length ? table.tBodies[0] : null;
    let rows = Array.from((tbody || table).rows);
    if(!tbody) rows = rows.slice(1);
    
    let rowNumber = 1;
    rows.forEach(row => {
      const rowNumCell = row.querySelector('.row-num-col');
      // Only number regular rows, skip summary rows and non-sortable rows
      if (rowNumCell && !row.classList.contains('non-sortable-row') && !row.classList.contains('summary-row')) {
        rowNumCell.textContent = rowNumber++;
      }
    });
  }

  // Export for external use
  window.TableSort = {
    parseDate: parseDate,
    parseTimeToSeconds: parseTimeToSeconds,
    makeSortable: makeSortable,
    sortByColumn: sortByColumn,
    resortTable: resortTable,
    updateRowNumbers: updateRowNumbers
  };

  document.addEventListener('DOMContentLoaded', function(){
    document.querySelectorAll('table.js-sortable').forEach(makeSortable);
  });
})();