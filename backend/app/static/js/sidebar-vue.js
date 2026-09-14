(function () {
  "use strict";

  if (!window.Vue) return;

  const normalizePath = (value) => {
    const path = String(value || "/").split("?")[0].replace(/\/+$/, "");
    return path || "/";
  };

  const menuSource = document.getElementById("sidebar-menu-data");
  if (!menuSource) return;

  let menuItems = [];
  try {
    menuItems = JSON.parse(menuSource.textContent || "[]");
  } catch (_) {
    menuItems = [];
  }

  const createSidebarApp = (root) => {
    const isMobile = root.dataset.mobile === "true";
    window.Vue.createApp({
      data() {
        return {
          items: menuItems,
          query: "",
          collapsed: !isMobile && localStorage.getItem("sidebarCollapsed") === "1",
          currentPath: normalizePath(window.location.pathname),
        };
      },
      computed: {
        filteredItems() {
          const query = this.query.trim().toLocaleLowerCase("es");
          if (!query) return this.items;
          return this.items.filter((item) =>
            `${item.label || ""} ${item.keywords || ""}`.toLocaleLowerCase("es").includes(query)
          );
        },
        activeHref() {
          const matches = this.items.filter((item) => {
            const href = normalizePath(item.href);
            return href === this.currentPath || (href !== "/home" && this.currentPath.startsWith(`${href}/`));
          });
          matches.sort((a, b) => normalizePath(b.href).length - normalizePath(a.href).length);
          return matches[0] ? normalizePath(matches[0].href) : "";
        },
      },
      mounted() {
        if (!isMobile) this.applyCollapsed();
      },
      methods: {
        isActive(item) {
          return normalizePath(item.href) === this.activeHref;
        },
        toggleSidebar() {
          this.collapsed = !this.collapsed;
          localStorage.setItem("sidebarCollapsed", this.collapsed ? "1" : "0");
          this.applyCollapsed();
        },
        applyCollapsed() {
          document.getElementById("layout")?.classList.toggle("sidebar-collapsed-bs", this.collapsed);
        },
        clearSearch() {
          this.query = "";
          this.$nextTick(() => this.$refs.search?.focus());
        },
      },
    }).mount(root);
  };

  document.querySelectorAll("[data-vue-sidebar]").forEach(createSidebarApp);
})();
