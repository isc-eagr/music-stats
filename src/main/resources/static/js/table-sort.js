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
    // Find header cells (thead th or first row th)
    let headerCells = Array.from(table.querySelectorAll('thead th'));
    if(headerCells.length === 0){
      const firstRow = table.rows && table.rows[0];
      if(firstRow){
        headerCells = Array.from(firstRow.querySelectorAll('th'));
      }
    }
    if(headerCells.length === 0) return; // nothing to do

    // Track current sort state
    let currentSortCol = null;
    let currentSortDir = null;

    headerCells.forEach((th, idx)=>{
      // Skip non-sortable columns (those without 'sortable' class if explicitly marked)
      // or columns marked with data-no-sort
      if(th.hasAttribute('data-no-sort')) return;
      
      // Only add click handler if column should be sortable
      const isSortable = th.classList.contains('sortable') || !th.classList.contains('no-sort');
      if(!isSortable) return;

      th.classList.add('sortable');
      th.style.cursor = 'pointer';
      
      th.addEventListener('click', ()=>{
        sortByColumn(table, idx, th, headerCells);
      });
    });

    // Apply default sort if specified
    const defaultSortTh = headerCells.find(th => th.hasAttribute('data-default-sort'));
    if(defaultSortTh){
      const idx = headerCells.indexOf(defaultSortTh);
      const dir = defaultSortTh.getAttribute('data-default-sort') || 'desc';
      defaultSortTh.setAttribute('data-sort-dir', dir === 'desc' ? 'asc' : 'desc'); // will be toggled
      sortByColumn(table, idx, defaultSortTh, headerCells);
    }
  }

  function sortByColumn(table, colIndex, th, allHeaders){
    // Toggle sort direction
    const currentDir = th.getAttribute('data-sort-dir');
    const dir = currentDir === 'asc' ? 'desc' : 'asc';
    const dataType = th.getAttribute('data-type') || null;

    // Clear other headers state
    allHeaders.forEach(h=>{
      h.removeAttribute('data-sort-dir');
      h.classList.remove('sorted-asc','sorted-desc','sort-asc','sort-desc');
    });
    th.setAttribute('data-sort-dir', dir);
    th.classList.add(dir === 'asc' ? 'sorted-asc' : 'sorted-desc');
    th.classList.add(dir === 'asc' ? 'sort-asc' : 'sort-desc');

    // Collect rows: if there's a tbody, take all its rows; otherwise skip header row
    const tbody = table.tBodies && table.tBodies.length ? table.tBodies[0] : null;
    let rows = Array.from((tbody || table).rows);
    if(!tbody) rows = rows.slice(1);

    // Filter out special rows (like summary rows)
    const regularRows = rows.filter(r => !r.classList.contains('non-sortable-row') && !r.id);
    const specialRows = rows.filter(r => r.classList.contains('non-sortable-row') || (r.id && r.id.includes('Summary')));

    // Determine type by sampling if not explicitly provided
    let sampleType = dataType || 'string';
    if(!dataType){
      for(const r of regularRows){
        const parsed = getCellValue(r.cells[colIndex] || {}, null);
        if(parsed.value !== '' && parsed.type !== 'string'){ 
          sampleType = parsed.type; 
          break; 
        }
      }
    }

    regularRows.sort((a,b)=>{
      const pa = getCellValue(a.cells[colIndex] || {}, dataType || sampleType);
      const pb = getCellValue(b.cells[colIndex] || {}, dataType || sampleType);

      // Handle empty values - sort to end
      const aEmpty = pa.value === '' || pa.value === 0;
      const bEmpty = pb.value === '' || pb.value === 0;
      if(aEmpty && !bEmpty) return 1;
      if(!aEmpty && bEmpty) return -1;

      if(sampleType === 'number' || sampleType === 'date'){
        const va = (pa.type==='number' || pa.type==='date') ? pa.value : 0;
        const vb = (pb.type==='number' || pb.type==='date') ? pb.value : 0;
        return dir === 'asc' ? (va - vb) : (vb - va);
      }
      // string compare
      const va = (pa.type==='string') ? pa.value : String(pa.value);
      const vb = (pb.type==='string') ? pb.value : String(pb.value);
      return dir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
    });

    // Re-append in sorted order, then special rows at end
    const parent = tbody || table;
    regularRows.forEach(r=> parent.appendChild(r));
    specialRows.forEach(r=> parent.appendChild(r));
    
    // Update row numbers after sorting
    updateRowNumbers(table);
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
    updateRowNumbers: updateRowNumbers
  };

  document.addEventListener('DOMContentLoaded', function(){
    document.querySelectorAll('table.js-sortable').forEach(makeSortable);
  });
})();