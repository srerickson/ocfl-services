// htmx
import htmx from 'htmx.org';
window.htmx = htmx;

// Alpine.js
import Alpine from 'alpinejs'
window.Alpine = Alpine

// Register dropdown component with keyboard navigation
document.addEventListener('alpine:init', () => {
	Alpine.data('dropdown', () => ({
		open: false,
		activeIndex: -1,

		toggle() {
			this.open = !this.open;
			if (this.open) {
				this.$nextTick(() => {
					this.focusFirstItem();
				});
			}
		},

		close() {
			this.open = false;
			this.activeIndex = -1;
		},

		focusFirstItem() {
			const items = this.$refs.menu.querySelectorAll('[role="menuitem"] a');
			if (items.length > 0) {
				items[0].focus();
				this.activeIndex = 0;
			}
		},

		handleKeydown(e) {
			const items = this.$refs.menu.querySelectorAll('[role="menuitem"] a');

			switch(e.key) {
				case 'ArrowDown':
					e.preventDefault();
					this.activeIndex = (this.activeIndex + 1) % items.length;
					items[this.activeIndex].focus();
					break;
				case 'ArrowUp':
					e.preventDefault();
					this.activeIndex = this.activeIndex <= 0 ? items.length - 1 : this.activeIndex - 1;
					items[this.activeIndex].focus();
					break;
				case 'Home':
					e.preventDefault();
					this.activeIndex = 0;
					items[0].focus();
					break;
				case 'End':
					e.preventDefault();
					this.activeIndex = items.length - 1;
					items[this.activeIndex].focus();
					break;
				case 'Escape':
					this.close();
					this.$refs.toggle.focus();
					break;
			}
		}
	}));
});

Alpine.start()

// Configure HTMX for better accessibility
document.addEventListener('DOMContentLoaded', function() {
	// Add aria-busy during HTMX requests
	document.body.addEventListener('htmx:beforeRequest', function(evt) {
		if (evt.detail.elt.hasAttribute('aria-live')) {
			evt.detail.elt.setAttribute('aria-busy', 'true');
		}
	});

	document.body.addEventListener('htmx:afterRequest', function(evt) {
		if (evt.detail.elt.hasAttribute('aria-live')) {
			evt.detail.elt.setAttribute('aria-busy', 'false');
		}
	});

	// Announce successful content loads to screen readers
	document.body.addEventListener('htmx:afterSwap', function(evt) {
		if (evt.detail.elt.hasAttribute('aria-live')) {
			const announcement = document.createElement('div');
			announcement.setAttribute('role', 'status');
			announcement.setAttribute('aria-live', 'polite');
			announcement.className = 'visually-hidden';
			announcement.textContent = 'Content loaded successfully';
			document.body.appendChild(announcement);

			setTimeout(() => announcement.remove(), 1000);
		}
	});
});
