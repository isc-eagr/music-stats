(function(){
  function parseTimeToSeconds(txt){
    // Supports mm:ss or hh:mm:ss
    const parts = txt.split(':').map(p=>p.trim());
    if(parts.length === 2){
      const [m,s] = parts.map(Number);
      if(!Number.isNaN(m) && !Number.isNaN(s)) return m*60 + s;
    }
    if(parts.length === 3){
      const [h,m,s] = parts.map(Number);
      if(!Number.isNaN(h) && !Number.isNaN(m) && !Number.isNaN(s)) return h*3600 + m*60 + s;
    }
    return NaN;
  }

  function parseDate(txt){
    // Parse "dd MMM yyyy" format (e.g., "21 Nov 2025")
    const monthMap = {
      'jan': 0, 'feb': 1, 'mar': 2, 'apr': 3, 'may': 4, 'jun': 5,
      'jul': 6, 'aug': 7, 'sep': 8, 'oct': 9, 'nov': 10, 'dec': 11
    };
    
    const parts = txt.trim().split(/\s+/);
    if(parts.length === 3){
      const day = parseInt(parts[0]);
      const month = monthMap[parts[1].toLowerCase()];
      const year = parseInt(parts[2]);
      
      if(!Number.isNaN(day) && month !== undefined && !Number.isNaN(year)){
        return new Date(year, month, day).getTime();
      }
    }
    return NaN;
  }

  function parseCell(text){
    if(!text) return {type:'string', value:''};
    const t = text.trim();
    if(!t) return {type:'string', value:''};

    // Date check (dd MMM yyyy)
    const dateVal = parseDate(t);
    if(!Number.isNaN(dateVal)) return {type:'date', value:dateVal};

    // Percent e.g. 12.34%
    if(t.endsWith('%')){
      const num = parseFloat(t.slice(0,-1));
      if(!Number.isNaN(num)) return {type:'number', value:num};
    }

    // Time like mm:ss or hh:mm:ss
    if(t.includes(':')){
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

  function getCellText(cell){
    // Prefer data-sort-value attribute if present
    const ds = cell.getAttribute && cell.getAttribute('data-sort-value');
    if(ds != null) return ds;
    return (cell.textContent || '').trim();
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

    headerCells.forEach((th, idx)=>{
      th.classList.add('sortable');
      th.addEventListener('click', ()=>{
        sortByColumn(table, idx, th);
      });
    });
  }

  function sortByColumn(table, colIndex, th){
    // Toggle sort direction
    const currentDir = th.getAttribute('data-sort-dir');
    const dir = currentDir === 'asc' ? 'desc' : 'asc';

    // Clear other headers state
    const allTh = table.querySelectorAll('th');
    allTh.forEach(h=>{
      h.removeAttribute('data-sort-dir');
      h.classList.remove('sorted-asc','sorted-desc');
    });
    th.setAttribute('data-sort-dir', dir);
    th.classList.add(dir === 'asc' ? 'sorted-asc' : 'sorted-desc');

    // Collect rows: if there's a tbody, take all its rows; otherwise skip header row
    const tbody = table.tBodies && table.tBodies.length ? table.tBodies[0] : null;
    let rows = Array.from((tbody || table).rows);
    if(!tbody) rows = rows.slice(1);

    // Determine type by sampling
    let sampleType = 'string';
    for(const r of rows){
      const t = getCellText(r.cells[colIndex] || {});
      const parsed = parseCell(t);
      if(parsed.value !== '' && t !== ''){ sampleType = parsed.type; break; }
    }

    rows.sort((a,b)=>{
      const ta = getCellText(a.cells[colIndex] || {});
      const tb = getCellText(b.cells[colIndex] || {});
      const pa = parseCell(ta);
      const pb = parseCell(tb);

      if(sampleType === 'number' || sampleType === 'date'){
        const va = (pa.type==='number' || pa.type==='date') ? pa.value : Number.NEGATIVE_INFINITY;
        const vb = (pb.type==='number' || pb.type==='date') ? pb.value : Number.NEGATIVE_INFINITY;
        return dir === 'asc' ? (va - vb) : (vb - va);
      }
      // string compare
      const va = (pa.type==='string') ? pa.value : String(pa.value);
      const vb = (pb.type==='string') ? pb.value : String(pb.value);
      return dir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
    });

    // Re-append
    const parent = tbody || table;
    rows.forEach(r=> parent.appendChild(r));
  }

  document.addEventListener('DOMContentLoaded', function(){
    document.querySelectorAll('table.js-sortable').forEach(makeSortable);
  });
})();