/**
 * Artist Associations tab: the collaboration map and the Collaborators table are one list.
 * Hovering a table row lights up its bubble (and dims the rest); hovering a bubble lights up its row.
 * Clicking a bubble jumps to its row instead of leaving the page (the row's name links to the artist).
 *
 * Markup contract (fragments/detail-associations.html :: artist):
 *   #assocPartners .as-map             the map (svg)
 *   .as-node[data-artist-id]           bubbles; href="#assocRow<id>"
 *   .as-edge[data-artist-id]           lines from the centre to each bubble
 *   tr[data-artist-id]#assocRow<id>    table rows
 */
(function () {
    function init() {
        const card = document.getElementById('assocPartners');
        if (!card) return;
        const map = card.querySelector('.as-map');
        if (!map) return;

        function light(id, on) {
            map.classList.toggle('has-hot', on);
            card.querySelectorAll('[data-artist-id="' + id + '"]').forEach(el => el.classList.toggle('is-hot', on));
        }

        card.querySelectorAll('[data-artist-id]').forEach(el => {
            const id = el.dataset.artistId;
            el.addEventListener('mouseenter', () => light(id, true));
            el.addEventListener('mouseleave', () => light(id, false));
        });

        map.querySelectorAll('.as-node').forEach(node => {
            node.addEventListener('click', event => {
                const row = document.getElementById('assocRow' + node.dataset.artistId);
                if (!row) return;
                event.preventDefault();
                light(node.dataset.artistId, false);
                row.scrollIntoView({ behavior: 'smooth', block: 'center' });
                row.classList.remove('is-flash');
                void row.offsetWidth;
                row.classList.add('is-flash');
            });
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
