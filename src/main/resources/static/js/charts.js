/**
 * Global Charts Module
 * Shared chart functions for gender breakdown charts across all list pages
 */

// Chart.js instances storage
let chartInstances = {};

// Track which tabs have been loaded
let loadedTabs = {
    general: false,
    top: false,
    genre: false,
    subgenre: false,
    ethnicity: false,
    language: false,
    country: false,
    releaseYear: false,
    listenYear: false
};

// Store top data for client-side sorting
let topTabData = {
    artists: [],
    albums: [],
    songs: []
};

// Current sort state for each table
let topSortState = {
    artists: { column: 'plays', direction: 'desc' },
    albums: { column: 'plays', direction: 'desc' },
    songs: { column: 'plays', direction: 'desc' }
};

// Register datalabels plugin if Chart.js is available
if (typeof Chart !== 'undefined' && typeof ChartDataLabels !== 'undefined') {
    Chart.register(ChartDataLabels);
}

/**
 * Format listening time in seconds to human-readable format
 * Matches the Java formatTime() method in SongService.java
 */
function formatListeningTime(totalSeconds) {
    if (!totalSeconds || totalSeconds === 0) {
        return '0m';
    }
    
    const days = Math.floor(totalSeconds / 86400);
    const hours = Math.floor((totalSeconds % 86400) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    
    if (days > 0) {
        return days + 'd ' + hours + 'h';
    } else if (hours > 0) {
        return hours + 'h ' + minutes + 'm';
    } else {
        return minutes + 'm';
    }
}

/**
 * Opens the charts modal and loads chart data
 */
function openChartsModal() {
    document.getElementById('chartsModal').classList.add('show');
    document.body.style.overflow = 'hidden';
    updateChartsFiltersDisplay();
    
    // Always default to General tab
    switchTab('general');
}

/**
 * Closes the charts modal
 */
function closeChartsModal() {
    document.getElementById('chartsModal').classList.remove('show');
    document.body.style.overflow = '';
}

/**
 * Switches to a specific tab and loads its data if not already loaded
 */
function switchTab(tabName) {
    // Update tab buttons
    document.querySelectorAll('.charts-tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === tabName);
    });
    
    // Update tab content visibility
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });
    document.getElementById('tab-' + tabName).classList.add('active');
    
    // Load data if not already loaded
    if (!loadedTabs[tabName]) {
        loadTabData(tabName);
    }
}

/**
 * Loads data for a specific tab
 */
function loadTabData(tabName, forceReload = false) {
    if (loadedTabs[tabName] && !forceReload) {
        return;
    }
    
    const loadingEl = document.getElementById('loading-' + tabName);
    const errorEl = document.getElementById('error-' + tabName);
    const contentEl = document.getElementById('content-' + tabName);
    
    // Show loading, hide error and content
    if (loadingEl) loadingEl.style.display = 'flex';
    if (errorEl) errorEl.style.display = 'none';
    if (contentEl) contentEl.style.display = 'none';
    
    // Build query string from current URL parameters
    const params = getFilterParams();
    
    // Add top limit for top tab
    if (tabName === 'top') {
        params.set('limit', getTopLimit());
    }
    
    const apiUrl = '/songs/api/charts/' + tabName + '?' + params.toString();
    
    console.log('Fetching chart data from:', apiUrl);
    
    fetch(apiUrl)
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok: ' + response.status);
            }
            return response.json();
        })
        .then(data => {
            console.log('Chart data received for', tabName + ':', data);
            
            // Hide loading, show content
            if (loadingEl) loadingEl.style.display = 'none';
            if (contentEl) contentEl.style.display = 'block';
            
            // Render charts based on tab
            renderTabCharts(tabName, data);
            loadedTabs[tabName] = true;
        })
        .catch(error => {
            console.error('Error loading chart data for', tabName + ':', error);
            if (loadingEl) loadingEl.style.display = 'none';
            if (errorEl) errorEl.style.display = 'flex';
        });
}

/**
 * Gets filter parameters from current URL
 */
function getFilterParams() {
    const currentUrl = new URL(window.location.href);
    const params = new URLSearchParams(currentUrl.search);
    
    // Remove pagination and sorting params, keep only filters
    params.delete('page');
    params.delete('perpage');
    params.delete('sortby');
    params.delete('sortdir');
    
    return params;
}

/**
 * Renders charts for a specific tab based on the data received
 */
function renderTabCharts(tabName, data) {
    switch (tabName) {
        case 'general':
            createCombinedBarChart('generalCombinedChartContainer', 'generalCombinedChart', {
                artists: [{ name: 'Overall', male: data.artistsByGender?.male || 0, female: data.artistsByGender?.female || 0 }],
                albums: [{ name: 'Overall', male: data.albumsByGender?.male || 0, female: data.albumsByGender?.female || 0 }],
                songs: [{ name: 'Overall', male: data.songsByGender?.male || 0, female: data.songsByGender?.female || 0 }],
                plays: [{ name: 'Overall', male: data.playsByGender?.male || 0, female: data.playsByGender?.female || 0 }],
                listeningTime: [{ name: 'Overall', male: data.listeningTimeByGender?.male || 0, female: data.listeningTimeByGender?.female || 0 }]
            });
            break;
        
        case 'top':
            renderTopTables(data);
            break;
            
        case 'genre':
            createCombinedBarChart('genreCombinedChartContainer', 'genreCombinedChart', {
                artists: data.artistsByGenre,
                albums: data.albumsByGenre,
                songs: data.songsByGenre,
                plays: data.playsByGenre,
                listeningTime: data.listeningTimeByGenre
            });
            break;
            
        case 'subgenre':
            createCombinedBarChart('subgenreCombinedChartContainer', 'subgenreCombinedChart', {
                artists: data.artistsBySubgenre,
                albums: data.albumsBySubgenre,
                songs: data.songsBySubgenre,
                plays: data.playsBySubgenre,
                listeningTime: data.listeningTimeBySubgenre
            });
            break;
            
        case 'ethnicity':
            createCombinedBarChart('ethnicityCombinedChartContainer', 'ethnicityCombinedChart', {
                artists: data.artistsByEthnicity,
                albums: data.albumsByEthnicity,
                songs: data.songsByEthnicity,
                plays: data.playsByEthnicity,
                listeningTime: data.listeningTimeByEthnicity
            });
            break;
            
        case 'language':
            createCombinedBarChart('languageCombinedChartContainer', 'languageCombinedChart', {
                artists: data.artistsByLanguage,
                albums: data.albumsByLanguage,
                songs: data.songsByLanguage,
                plays: data.playsByLanguage,
                listeningTime: data.listeningTimeByLanguage
            });
            break;
            
        case 'country':
            createCombinedBarChart('countryCombinedChartContainer', 'countryCombinedChart', {
                artists: data.artistsByCountry,
                albums: data.albumsByCountry,
                songs: data.songsByCountry,
                plays: data.playsByCountry,
                listeningTime: data.listeningTimeByCountry
            });
            break;
            
        case 'releaseYear':
            createCombinedBarChart('releaseYearCombinedChartContainer', 'releaseYearCombinedChart', {
                artists: null, // No artists for release year
                albums: data.albumsByReleaseYear,
                songs: data.songsByReleaseYear,
                plays: data.playsByReleaseYear,
                listeningTime: data.listeningTimeByReleaseYear
            });
            break;
            
        case 'listenYear':
            createCombinedBarChart('listenYearCombinedChartContainer', 'listenYearCombinedChart', {
                artists: data.artistsByListenYear,
                albums: data.albumsByListenYear,
                songs: data.songsByListenYear,
                plays: data.playsByListenYear,
                listeningTime: data.listeningTimeByListenYear
            });
            break;
    }
}

/**
 * Updates the filters display in the charts modal
 * Copies filter chips from the page's active filters section
 */
function updateChartsFiltersDisplay() {
    const container = document.getElementById('chartsFiltersDisplay');
    const noFiltersMsg = document.getElementById('noFiltersMsg');
    
    if (!container) return;
    
    // Remove existing filter chips from charts modal
    container.querySelectorAll('.filter-chip').forEach(el => el.remove());
    
    // Copy filter chips from the page's active filters section
    const pageFilters = document.getElementById('activeFilters');
    const pageChips = pageFilters ? pageFilters.querySelectorAll('.filter-chip') : [];
    
    if (pageChips.length === 0) {
        if (noFiltersMsg) noFiltersMsg.style.display = 'inline';
    } else {
        if (noFiltersMsg) noFiltersMsg.style.display = 'none';
        pageChips.forEach(chip => {
            const label = chip.querySelector('.filter-chip-label');
            const values = chip.querySelectorAll('.filter-chip-value');
            if (label) {
                const newChip = document.createElement('span');
                newChip.className = 'filter-chip';
                
                // Create label span
                const labelSpan = document.createElement('span');
                labelSpan.className = 'filter-chip-label';
                labelSpan.textContent = label.textContent;
                newChip.appendChild(labelSpan);
                
                // Add values with bold styling
                if (values.length > 0) {
                    values.forEach((value, idx) => {
                        const valueSpan = document.createElement('span');
                        valueSpan.className = 'filter-chip-value filter-value-highlight';
                        valueSpan.textContent = value.textContent.trim();
                        newChip.appendChild(valueSpan);
                    });
                }
                
                container.appendChild(newChip);
            }
        });
    }
}

/**
 * Creates a gender pie/doughnut chart
 * @param {string} canvasId - The canvas element ID
 * @param {object} genderData - Object with male and female counts
 * @param {string} label - Label for the chart
 * @param {boolean} isListeningTime - Whether this is a listening time chart (format values differently)
 */
function createGenderPieChart(canvasId, genderData, label, isListeningTime = false) {
    if (chartInstances[canvasId]) {
        chartInstances[canvasId].destroy();
    }
    
    // Handle null/undefined data
    if (!genderData) {
        console.warn('No gender data for chart:', canvasId);
        genderData = { male: 0, female: 0 };
    }
    
    const canvas = document.getElementById(canvasId);
    if (!canvas) {
        console.warn('Canvas not found:', canvasId);
        return;
    }
    
    const ctx = canvas.getContext('2d');
    chartInstances[canvasId] = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Male', 'Female'],
            datasets: [{
                data: [genderData.male || 0, genderData.female || 0],
                backgroundColor: ['rgba(59, 130, 246, 0.8)', 'rgba(236, 72, 153, 0.8)'],
                borderColor: ['rgba(59, 130, 246, 1)', 'rgba(236, 72, 153, 1)'],
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        color: '#e5e7eb',
                        padding: 16,
                        font: { size: 12 }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const value = context.raw;
                            const percentage = total > 0 ? ((value / total) * 100).toFixed(2) : '0.00';
                            const displayValue = isListeningTime ? formatListeningTime(value) : value.toLocaleString();
                            return context.label + ': ' + displayValue + ' (' + percentage + '%)';
                        }
                    }
                },
                datalabels: {
                    color: '#fff',
                    font: { weight: 'bold', size: 14 },
                    formatter: (value, ctx) => {
                        const total = ctx.dataset.data.reduce((a, b) => a + b, 0);
                        if (total === 0 || value === 0) return '';
                        const pct = ((value / total) * 100).toFixed(2);
                        return pct + '%';
                    }
                }
            }
        }
    });
}

/**
 * Creates a dynamically-sized stacked bar chart
 * @param {string} containerId - The container div ID for the canvas
 * @param {string} canvasId - The canvas element ID to create
 * @param {array} categoryData - Array of objects with name, male, female properties
 * @param {boolean} isListeningTime - Whether this is a listening time chart
 */
function createDynamicStackedBarChart(containerId, canvasId, categoryData, isListeningTime = false) {
    if (chartInstances[canvasId]) {
        chartInstances[canvasId].destroy();
    }
    
    if (!categoryData || categoryData.length === 0) {
        categoryData = [{ name: 'No data', male: 0, female: 0 }];
    }
    
    const container = document.getElementById(containerId);
    if (!container) {
        console.warn('Container not found:', containerId);
        return;
    }
    
    // Calculate dynamic height based on number of items
    const itemHeight = 35;
    const baseHeight = 100;
    const calculatedHeight = categoryData.length * itemHeight + baseHeight;
    
    // Create canvas element
    container.innerHTML = '';
    const canvas = document.createElement('canvas');
    canvas.id = canvasId;
    canvas.style.height = calculatedHeight + 'px';
    container.appendChild(canvas);
    
    const ctx = canvas.getContext('2d');
    const labels = categoryData.map(item => item.name);
    const maleData = categoryData.map(item => item.male || 0);
    const femaleData = categoryData.map(item => item.female || 0);
    
    chartInstances[canvasId] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Male',
                    data: maleData,
                    backgroundColor: 'rgba(59, 130, 246, 0.8)',
                    borderColor: 'rgba(59, 130, 246, 1)',
                    borderWidth: 1
                },
                {
                    label: 'Female',
                    data: femaleData,
                    backgroundColor: 'rgba(236, 72, 153, 0.8)',
                    borderColor: 'rgba(236, 72, 153, 1)',
                    borderWidth: 1
                }
            ]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    stacked: true,
                    ticks: { 
                        color: '#e5e7eb',
                        callback: function(value) {
                            if (isListeningTime) {
                                return formatListeningTime(value);
                            }
                            return value.toLocaleString();
                        }
                    },
                    grid: { color: 'rgba(255, 255, 255, 0.05)' }
                },
                y: {
                    stacked: true,
                    ticks: { color: '#e5e7eb' },
                    grid: { color: 'rgba(255, 255, 255, 0.05)' }
                }
            },
            plugins: {
                legend: {
                    position: 'top',
                    labels: {
                        color: '#e5e7eb',
                        padding: 16,
                        font: { size: 12 }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const dataIndex = context.dataIndex;
                            const male = maleData[dataIndex] || 0;
                            const female = femaleData[dataIndex] || 0;
                            const total = male + female;
                            const pct = total > 0 ? ((context.raw / total) * 100).toFixed(2) : '0.00';
                            const displayValue = isListeningTime ? formatListeningTime(context.raw) : context.raw.toLocaleString();
                            return context.dataset.label + ': ' + displayValue + ' (' + pct + '%)';
                        }
                    }
                },
                datalabels: {
                    color: '#fff',
                    font: { weight: 'bold', size: 11 },
                    anchor: 'center',
                    align: 'center',
                    formatter: (value, ctx) => {
                        const dataIndex = ctx.dataIndex;
                        const male = maleData[dataIndex] || 0;
                        const female = femaleData[dataIndex] || 0;
                        const total = male + female;
                        if (total === 0 || value === 0) return '';
                        const pct = ((value / total) * 100).toFixed(2);
                        if (parseFloat(pct) < 5) return ''; // Hide very small percentages
                        return pct + '%';
                    }
                }
            }
        }
    });
}

/**
 * Creates a combined bar chart showing all 5 metrics (Artists, Albums, Songs, Plays, Listening Time) for each category
 * Each category (e.g., "Pop") gets 5 grouped bars, one for each metric
 * @param {string} containerId - The container div ID
 * @param {string} canvasId - The canvas element ID to create
 * @param {object} allData - Object with artists, albums, songs, plays, listeningTime arrays
 */
function createCombinedBarChart(containerId, canvasId, allData) {
    if (chartInstances[canvasId]) {
        chartInstances[canvasId].destroy();
    }
    
    const container = document.getElementById(containerId);
    if (!container) {
        console.warn('Container not found:', containerId);
        return;
    }
    
    // Get all unique category names from all datasets
    const categorySet = new Set();
    ['artists', 'albums', 'songs', 'plays', 'listeningTime'].forEach(metric => {
        if (allData[metric]) {
            allData[metric].forEach(item => categorySet.add(item.name));
        }
    });
    let categories = Array.from(categorySet);
    
    // Determine if this is a year-based chart (Release Year or Listen Year)
    const isYearChart = canvasId.includes('releaseYear') || canvasId.includes('listenYear');
    
    // Sort categories
    if (isYearChart) {
        // For year charts, sort by year descending (newest first)
        categories.sort((a, b) => {
            // Handle 'Unknown' - put it last
            if (a === 'Unknown') return 1;
            if (b === 'Unknown') return -1;
            // Sort years descending
            const yearA = parseInt(a);
            const yearB = parseInt(b);
            return yearB - yearA;
        });
    } else {
        // For non-year charts, sort by total plays descending
        if (allData.plays) {
            const playsMap = {};
            allData.plays.forEach(item => {
                playsMap[item.name] = (item.male || 0) + (item.female || 0) + (item.other || 0);
            });
            categories.sort((a, b) => {
                const playsA = playsMap[a] || 0;
                const playsB = playsMap[b] || 0;
                return playsB - playsA; // Descending order
            });
        }
    }
    
    if (categories.length === 0) {
        container.innerHTML = '<div style="text-align: center; color: #666; padding: 40px;">No data available</div>';
        return;
    }
    
    // Create lookup maps for each metric
    const createLookup = (data) => {
        if (!data) return {};
        const map = {};
        data.forEach(item => {
            map[item.name] = { male: item.male || 0, female: item.female || 0, other: item.other || 0 };
        });
        return map;
    };
    
    const artistsLookup = createLookup(allData.artists);
    const albumsLookup = createLookup(allData.albums);
    const songsLookup = createLookup(allData.songs);
    const playsLookup = createLookup(allData.plays);
    const listeningTimeLookup = createLookup(allData.listeningTime);
    
    // Calculate dynamic height - more space per category since we have 5 bars per category
    const itemHeight = 120; // Height per category group
    const baseHeight = 150;
    const calculatedHeight = categories.length * itemHeight + baseHeight;
    
    // Create canvas element
    container.innerHTML = '';
    const canvas = document.createElement('canvas');
    canvas.id = canvasId;
    canvas.style.height = calculatedHeight + 'px';
    container.appendChild(canvas);
    
    const ctx = canvas.getContext('2d');
    
    // Define colors for each metric (blue shades for male, pink shades for female, gray shades for other)
    const metricColors = {
        artists: { male: 'rgba(30, 64, 175, 0.9)', female: 'rgba(190, 24, 93, 0.9)', other: 'rgba(75, 85, 99, 0.9)' },       // Dark blue / Dark pink / Dark gray
        albums: { male: 'rgba(59, 130, 246, 0.9)', female: 'rgba(236, 72, 153, 0.9)', other: 'rgba(107, 114, 128, 0.9)' },      // Medium blue / Medium pink / Medium gray
        songs: { male: 'rgba(96, 165, 250, 0.9)', female: 'rgba(244, 114, 182, 0.9)', other: 'rgba(156, 163, 175, 0.9)' },      // Light blue / Light pink / Light gray
        plays: { male: 'rgba(147, 197, 253, 0.9)', female: 'rgba(249, 168, 212, 0.9)', other: 'rgba(209, 213, 219, 0.9)' },     // Lighter blue / Lighter pink / Lighter gray
        listeningTime: { male: 'rgba(191, 219, 254, 0.9)', female: 'rgba(252, 231, 243, 0.9)', other: 'rgba(229, 231, 235, 0.9)' } // Lightest blue / Lightest pink / Lightest gray
    };
    
    // Build datasets - each metric gets male and female bars, normalized to percentages
    const datasets = [];
    const metrics = [
        { key: 'artists', label: 'Artists', lookup: artistsLookup },
        { key: 'albums', label: 'Albums', lookup: albumsLookup },
        { key: 'songs', label: 'Songs', lookup: songsLookup },
        { key: 'plays', label: 'Plays', lookup: playsLookup },
        { key: 'listeningTime', label: 'Listen Time', lookup: listeningTimeLookup, isTime: true }
    ];
    
    // Store raw values for tooltips and totals
    const rawValues = {};
    const totalsByMetric = {}; // Store totals for each metric per category

    metrics.forEach((metric, idx) => {
        if (!allData[metric.key]) return; // Skip if no data (e.g., artists for release year)
        
        const maleData = categories.map(cat => metric.lookup[cat]?.male || 0);
        const femaleData = categories.map(cat => metric.lookup[cat]?.female || 0);
        const otherData = categories.map(cat => metric.lookup[cat]?.other || 0);
        
        // Calculate total for each category
        const totals = categories.map((cat, i) => maleData[i] + femaleData[i] + otherData[i]);
        totalsByMetric[metric.key] = totals;
        
        // Calculate percentages (including other)
        const malePercentages = categories.map((cat, i) => {
            const total = totals[i];
            return total > 0 ? (maleData[i] / total) * 100 : 0;
        });
        const femalePercentages = categories.map((cat, i) => {
            const total = totals[i];
            return total > 0 ? (femaleData[i] / total) * 100 : 0;
        });
        const otherPercentages = categories.map((cat, i) => {
            const total = totals[i];
            return total > 0 ? (otherData[i] / total) * 100 : 0;
        });
        
        // Store raw values for tooltips
        rawValues[metric.key] = { male: maleData, female: femaleData, other: otherData, totals: totals, isTime: metric.isTime };
        
        // Male bar for this metric
        datasets.push({
            label: metric.label + ' (M)',
            data: malePercentages,
            backgroundColor: metricColors[metric.key].male,
            borderColor: metricColors[metric.key].male.replace('0.9', '1'),
            borderWidth: 1,
            stack: 'stack' + idx,
            metricKey: metric.key,
            isTime: metric.isTime || false,
            genderType: 'male',
            rawData: maleData,
            totalData: totals
        });
        
        // Female bar for this metric
        datasets.push({
            label: metric.label + ' (F)',
            data: femalePercentages,
            backgroundColor: metricColors[metric.key].female,
            borderColor: metricColors[metric.key].female.replace('0.9', '1'),
            borderWidth: 1,
            stack: 'stack' + idx,
            metricKey: metric.key,
            isTime: metric.isTime || false,
            genderType: 'female',
            rawData: femaleData,
            totalData: totals
        });
        
        // Other bar for this metric (only add if there's any "other" data)
        const hasOtherData = otherData.some(v => v > 0);
        if (hasOtherData) {
            datasets.push({
                label: metric.label + ' (O)',
                data: otherPercentages,
                backgroundColor: metricColors[metric.key].other,
                borderColor: metricColors[metric.key].other.replace('0.9', '1'),
                borderWidth: 1,
                stack: 'stack' + idx,
                metricKey: metric.key,
                isTime: metric.isTime || false,
                genderType: 'other',
                rawData: otherData,
                totalData: totals
            });
        }
    });
    
    chartInstances[canvasId] = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: categories,
            datasets: datasets
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            layout: {
                padding: {
                    left: 0 // Totals are drawn inside the bar area now
                }
            },
            scales: {
                x: {
                    stacked: true,
                    min: 0,
                    max: 100,
                    ticks: { 
                        color: '#e5e7eb',
                        callback: function(value) {
                            return value + '%';
                        }
                    },
                    grid: { color: 'rgba(255, 255, 255, 0.05)' }
                },
                y: {
                    stacked: true,
                    ticks: { 
                        color: '#e5e7eb',
                        font: { size: 12, weight: 'bold' }
                    },
                    grid: { color: 'rgba(255, 255, 255, 0.05)' }
                }
            },
            plugins: {
                legend: {
                    position: 'top',
                    labels: {
                        color: '#e5e7eb',
                        padding: 12,
                        font: { size: 11 },
                        usePointStyle: true,
                        boxWidth: 12,
                        filter: function(legendItem, chartData) {
                            // Remove duplicate legend entries (only show one per metric type)
                            const label = legendItem.text;
                            // Keep M, F, O entries but group them visually
                            return true;
                        }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const isTime = context.dataset.isTime;
                            const rawValue = context.dataset.rawData[context.dataIndex];
                            const totalValue = context.dataset.totalData[context.dataIndex];
                            const displayValue = isTime ? formatListeningTime(rawValue) : rawValue.toLocaleString();
                            const displayTotal = isTime ? formatListeningTime(totalValue) : totalValue.toLocaleString();
                            const pct = context.raw.toFixed(2);
                            
                            return context.dataset.label + ': ' + displayValue + ' (' + pct + '%) | Total: ' + displayTotal;
                        }
                    }
                },
                datalabels: {
                    color: function(context) {
                        // Use darker text for lighter backgrounds and for "other" bars
                        const metricKey = context.dataset.metricKey;
                        const genderType = context.dataset.genderType;
                        if (metricKey === 'listeningTime' || metricKey === 'plays' || genderType === 'other') {
                            return '#333';
                        }
                        return '#fff';
                    },
                    font: { weight: 'bold', size: 9 },
                    anchor: 'center',
                    align: 'center',
                    formatter: (value, ctx) => {
                        if (value < 5) return ''; // Hide small percentages
                        const rawValue = ctx.dataset.rawData[ctx.dataIndex];
                        const isTime = ctx.dataset.isTime;
                        const displayValue = isTime ? formatListeningTime(rawValue) : rawValue.toLocaleString();
                        return displayValue + ' (' + value.toFixed(2) + '%)';
                    }
                }
            }
        },
        plugins: [{
            // Custom plugin to draw totals at the beginning of each bar
            id: 'barTotals',
            afterDraw: function(chart) {
                const ctx = chart.ctx;
                const yAxis = chart.scales.y;
                const xAxis = chart.scales.x;
                
                // Group datasets by their stack (each stack represents one metric)
                const stackGroups = {};
                chart.data.datasets.forEach((dataset, datasetIndex) => {
                    const stack = dataset.stack;
                    if (!stackGroups[stack]) {
                        stackGroups[stack] = {
                            metricKey: dataset.metricKey,
                            isTime: dataset.isTime,
                            totalData: dataset.totalData
                        };
                    }
                });
                
                // For each category (y-axis label), draw totals for each metric
                chart.data.labels.forEach((label, categoryIndex) => {
                    const yPos = yAxis.getPixelForValue(categoryIndex);
                    
                    // Get the number of stacks (metrics)
                    const stackKeys = Object.keys(stackGroups);
                    const numStacks = stackKeys.length;
                    
                    // Calculate the bar height per metric within each category
                    const categoryHeight = yAxis.height / chart.data.labels.length;
                    const barGroupHeight = categoryHeight * 0.8; // 80% of category height for bars
                    const singleBarHeight = barGroupHeight / numStacks;
                    
                    // Draw total for each metric in this category
                    stackKeys.forEach((stackKey, stackIndex) => {
                        const stackData = stackGroups[stackKey];
                        const total = stackData.totalData ? stackData.totalData[categoryIndex] : 0;
                        
                        if (total > 0) {
                            const displayTotal = stackData.isTime ? formatListeningTime(total) : total.toLocaleString();
                            
                            // Calculate y position for this specific bar within the category
                            const barYOffset = (stackIndex - (numStacks - 1) / 2) * singleBarHeight;
                            const barY = yPos + barYOffset;
                            
                            // Draw at the start of the bars (inside the chart area)
                            ctx.save();
                            ctx.font = 'bold 10px sans-serif';
                            // Draw text with dark outline for visibility on light backgrounds
                            ctx.strokeStyle = 'rgba(0, 0, 0, 0.7)';
                            ctx.lineWidth = 3;
                            ctx.textAlign = 'left';
                            ctx.textBaseline = 'middle';
                            ctx.strokeText(displayTotal, xAxis.left + 3, barY);
                            ctx.fillStyle = '#ffffff';
                            ctx.fillText(displayTotal, xAxis.left + 3, barY);
                            ctx.restore();
                        }
                    });
                });
            }
        }]
    });
}

/**
 * Resets the charts loaded flags so charts will reload on next open
 * Call this when filters change
 */
function resetChartsLoaded() {
    loadedTabs = {
        general: false,
        top: false,
        genre: false,
        subgenre: false,
        ethnicity: false,
        language: false,
        releaseYear: false,
        listenYear: false
    };
}

/**
 * Renders the Top tables with artist, album, and song data
 */
function renderTopTables(data) {
    // Store data for client-side sorting
    topTabData.artists = data.topArtists || [];
    topTabData.albums = data.topAlbums || [];
    topTabData.songs = data.topSongs || [];
    
    // Render each table
    renderTopArtistsTable();
    renderTopAlbumsTable();
    renderTopSongsTable();
    
    // Setup sorting handlers
    setupTopTableSorting();
}

/**
 * Renders the Top Artists table
 */
function renderTopArtistsTable() {
    const tbody = document.querySelector('#topArtistsTable tbody');
    if (!tbody) return;
    
    const data = sortTopData(topTabData.artists, topSortState.artists);
    
    if (data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="14" style="text-align: center; color: #666; padding: 20px;">No artist data available</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.map((artist, index) => `
        <tr>
            <td class="rank-col">${index + 1}</td>
            <td class="cover-col">
                <a href="/artists/${artist.id}">
                    <img src="/artists/${artist.id}/image" alt="" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" style="width:32px;height:32px;object-fit:cover;border-radius:50%;">
                    <div style="display:none;width:32px;height:32px;background:#2a2a2a;border-radius:50%;align-items:center;justify-content:center;color:#666;font-size:14px;">🎤</div>
                </a>
            </td>
            <td><a href="/artists/${artist.id}">${escapeHtml(artist.name || '-')}</a></td>
            <td>${artist.firstListened || '-'}</td>
            <td>${artist.lastListened || '-'}</td>
            <td class="numeric">${(artist.primaryPlays || 0).toLocaleString()}</td>
            <td class="numeric">${(artist.legacyPlays || 0).toLocaleString()}</td>
            <td class="numeric">${(artist.plays || 0).toLocaleString()}</td>
            <td>${artist.genreId ? `<a href="/genres/${artist.genreId}">${escapeHtml(artist.genre)}</a>` : '-'}</td>
            <td>${artist.subgenreId ? `<a href="/subgenres/${artist.subgenreId}">${escapeHtml(artist.subgenre)}</a>` : '-'}</td>
            <td>${artist.ethnicityId ? `<a href="/ethnicities/${artist.ethnicityId}">${escapeHtml(artist.ethnicity)}</a>` : '-'}</td>
            <td>${artist.languageId ? `<a href="/languages/${artist.languageId}">${escapeHtml(artist.language)}</a>` : '-'}</td>
            <td>${escapeHtml(artist.country || '-')}</td>
            <td class="numeric">${artist.timeListenedFormatted || '-'}</td>
        </tr>
    `).join('');
    
    updateSortIndicators('topArtistsTable', topSortState.artists);
}

/**
 * Renders the Top Albums table
 */
function renderTopAlbumsTable() {
    const tbody = document.querySelector('#topAlbumsTable tbody');
    if (!tbody) return;
    
    const data = sortTopData(topTabData.albums, topSortState.albums);
    
    if (data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="16" style="text-align: center; color: #666; padding: 20px;">No album data available</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.map((album, index) => `
        <tr>
            <td class="rank-col">${index + 1}</td>
            <td class="cover-col">
                <a href="/albums/${album.id}">
                    <img src="/albums/${album.id}/image" alt="" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" style="width:32px;height:32px;object-fit:cover;border-radius:4px;">
                    <div style="display:none;width:32px;height:32px;background:#2a2a2a;border-radius:4px;align-items:center;justify-content:center;color:#666;font-size:14px;">💿</div>
                </a>
            </td>
            <td><a href="/artists/${album.artistId}">${escapeHtml(album.artistName || '-')}</a></td>
            <td><a href="/albums/${album.id}">${escapeHtml(album.name || '-')}</a></td>
            <td>${album.releaseDate || '-'}</td>
            <td>${album.firstListened || '-'}</td>
            <td>${album.lastListened || '-'}</td>
            <td class="numeric">${(album.primaryPlays || 0).toLocaleString()}</td>
            <td class="numeric">${(album.legacyPlays || 0).toLocaleString()}</td>
            <td class="numeric">${(album.plays || 0).toLocaleString()}</td>
            <td>${album.genreId ? `<a href="/genres/${album.genreId}">${escapeHtml(album.genre)}</a>` : '-'}</td>
            <td>${album.subgenreId ? `<a href="/subgenres/${album.subgenreId}">${escapeHtml(album.subgenre)}</a>` : '-'}</td>
            <td>${album.ethnicityId ? `<a href="/ethnicities/${album.ethnicityId}">${escapeHtml(album.ethnicity)}</a>` : '-'}</td>
            <td>${album.languageId ? `<a href="/languages/${album.languageId}">${escapeHtml(album.language)}</a>` : '-'}</td>
            <td>${escapeHtml(album.country || '-')}</td>
            <td class="numeric">${album.timeListenedFormatted || '-'}</td>
        </tr>
    `).join('');
    
    updateSortIndicators('topAlbumsTable', topSortState.albums);
}

/**
 * Renders the Top Songs table
 */
function renderTopSongsTable() {
    const tbody = document.querySelector('#topSongsTable tbody');
    if (!tbody) return;
    
    const data = sortTopData(topTabData.songs, topSortState.songs);
    
    if (data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="17" style="text-align: center; color: #666; padding: 20px;">No song data available</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.map((song, index) => {
        const coverHtml = song.albumId 
            ? `<a href="/albums/${song.albumId}">
                   <img src="/albums/${song.albumId}/image" alt="" class="inherited-cover" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" style="width:32px;height:32px;object-fit:cover;border-radius:4px;">
                   <div style="display:none;width:32px;height:32px;background:#2a2a2a;border-radius:4px;align-items:center;justify-content:center;color:#666;font-size:14px;">💿</div>
               </a>`
            : `<div style="width:32px;height:32px;background:#2a2a2a;border-radius:4px;display:flex;align-items:center;justify-content:center;color:#666;font-size:14px;">🎵</div>`;
        return `
        <tr>
            <td class="rank-col">${index + 1}</td>
            <td class="cover-col">${coverHtml}</td>
            <td><a href="/artists/${song.artistId}">${escapeHtml(song.artistName || '-')}</a></td>
            <td>${song.albumId ? `<a href="/albums/${song.albumId}">${escapeHtml(song.albumName)}</a>` : '-'}</td>
            <td><a href="/songs/${song.id}">${escapeHtml(song.name || '-')}</a></td>
            <td>${song.releaseDate || '-'}</td>
            <td>${song.firstListened || '-'}</td>
            <td>${song.lastListened || '-'}</td>
            <td class="numeric">${(song.primaryPlays || 0).toLocaleString()}</td>
            <td class="numeric">${(song.legacyPlays || 0).toLocaleString()}</td>
            <td class="numeric">${(song.plays || 0).toLocaleString()}</td>
            <td>${song.genreId ? `<a href="/genres/${song.genreId}">${escapeHtml(song.genre)}</a>` : '-'}</td>
            <td>${song.subgenreId ? `<a href="/subgenres/${song.subgenreId}">${escapeHtml(song.subgenre)}</a>` : '-'}</td>
            <td>${song.ethnicityId ? `<a href="/ethnicities/${song.ethnicityId}">${escapeHtml(song.ethnicity)}</a>` : '-'}</td>
            <td>${song.languageId ? `<a href="/languages/${song.languageId}">${escapeHtml(song.language)}</a>` : '-'}</td>
            <td>${escapeHtml(song.country || '-')}</td>
            <td class="numeric">${song.timeListenedFormatted || '-'}</td>
        </tr>
    `}).join('');
    
    updateSortIndicators('topSongsTable', topSortState.songs);
}

/**
 * Sorts top data array by the specified column and direction
 */
function sortTopData(data, sortState) {
    if (!data || data.length === 0) return [];
    
    const sorted = [...data];
    const { column, direction } = sortState;
    
    sorted.sort((a, b) => {
        let aVal = a[column];
        let bVal = b[column];
        
        // Handle null/undefined values
        if (aVal == null) aVal = '';
        if (bVal == null) bVal = '';
        
        // Numeric columns
        if (column === 'plays' || column === 'primaryPlays' || column === 'legacyPlays' || column === 'timeListened') {
            aVal = Number(aVal) || 0;
            bVal = Number(bVal) || 0;
        } else if (column === 'releaseDate' || column === 'firstListened' || column === 'lastListened') {
            // Parse date strings like "1 Feb 2012" or "15 Sep 2006"
            aVal = parseDisplayDate(aVal);
            bVal = parseDisplayDate(bVal);
        } else if (typeof aVal === 'string') {
            aVal = aVal.toLowerCase();
            bVal = bVal.toLowerCase();
        }
        
        let result = 0;
        if (aVal < bVal) result = -1;
        else if (aVal > bVal) result = 1;
        
        return direction === 'desc' ? -result : result;
    });
    
    return sorted;
}

/**
 * Parses a display date string like "1 Feb 2012" into a sortable timestamp
 */
function parseDisplayDate(dateStr) {
    if (!dateStr || dateStr === '') return 0;
    
    const months = {
        'jan': 0, 'feb': 1, 'mar': 2, 'apr': 3, 'may': 4, 'jun': 5,
        'jul': 6, 'aug': 7, 'sep': 8, 'oct': 9, 'nov': 10, 'dec': 11
    };
    
    const parts = dateStr.trim().split(' ');
    if (parts.length === 3) {
        const day = parseInt(parts[0], 10);
        const month = months[parts[1].toLowerCase().substring(0, 3)];
        const year = parseInt(parts[2], 10);
        if (!isNaN(day) && month !== undefined && !isNaN(year)) {
            return new Date(year, month, day).getTime();
        }
    }
    
    return 0;
}

/**
 * Updates sort indicators on table headers
 */
function updateSortIndicators(tableId, sortState) {
    const table = document.getElementById(tableId);
    if (!table) return;
    
    // Remove existing sort classes
    table.querySelectorAll('th').forEach(th => {
        th.classList.remove('sorted-asc', 'sorted-desc');
    });
    
    // Add sort class to current column
    const sortedTh = table.querySelector(`th[data-sort="${sortState.column}"]`);
    if (sortedTh) {
        sortedTh.classList.add(sortState.direction === 'asc' ? 'sorted-asc' : 'sorted-desc');
    }
}

/**
 * Sets up click handlers for table header sorting
 */
function setupTopTableSorting() {
    // Artists table
    document.querySelectorAll('#topArtistsTable th[data-sort]').forEach(th => {
        th.onclick = () => {
            const column = th.dataset.sort;
            if (topSortState.artists.column === column) {
                topSortState.artists.direction = topSortState.artists.direction === 'asc' ? 'desc' : 'asc';
            } else {
                topSortState.artists.column = column;
                topSortState.artists.direction = column === 'plays' || column === 'timeListened' ? 'desc' : 'asc';
            }
            renderTopArtistsTable();
        };
    });
    
    // Albums table
    document.querySelectorAll('#topAlbumsTable th[data-sort]').forEach(th => {
        th.onclick = () => {
            const column = th.dataset.sort;
            if (topSortState.albums.column === column) {
                topSortState.albums.direction = topSortState.albums.direction === 'asc' ? 'desc' : 'asc';
            } else {
                topSortState.albums.column = column;
                topSortState.albums.direction = column === 'plays' || column === 'timeListened' ? 'desc' : 'asc';
            }
            renderTopAlbumsTable();
        };
    });
    
    // Songs table
    document.querySelectorAll('#topSongsTable th[data-sort]').forEach(th => {
        th.onclick = () => {
            const column = th.dataset.sort;
            if (topSortState.songs.column === column) {
                topSortState.songs.direction = topSortState.songs.direction === 'asc' ? 'desc' : 'asc';
            } else {
                topSortState.songs.column = column;
                topSortState.songs.direction = column === 'plays' || column === 'timeListened' ? 'desc' : 'asc';
            }
            renderTopSongsTable();
        };
    });
}

/**
 * Reloads top data when the limit changes
 */
function reloadTopData() {
    loadedTabs.top = false;
    loadTabData('top', true);
}

/**
 * Gets the current top limit value
 */
function getTopLimit() {
    const input = document.getElementById('topLimitInput');
    return input ? parseInt(input.value) || 10 : 10;
}

/**
 * Escapes HTML special characters
 */
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Initialize modal close on click outside
document.addEventListener('DOMContentLoaded', function() {
    const modal = document.getElementById('chartsModal');
    if (modal) {
        modal.addEventListener('click', function(e) {
            if (e.target === this) {
                closeChartsModal();
            }
        });
    }
});
