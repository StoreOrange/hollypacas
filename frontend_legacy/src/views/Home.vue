<template>
  <div class="erp-shell">
    <aside class="erp-sidebar">
      <div class="brand">
        <div class="brand-mark">HP</div>
        <div>
          <div class="brand-name">Hollywood Pacas</div>
          <div class="brand-sub">ERP Central</div>
        </div>
      </div>

      <div class="nav-title">Modulos</div>
      <nav class="nav-stack">
        <component
          v-for="item in menuItems"
          :key="item.title"
          :is="item.route ? 'router-link' : 'button'"
          :to="item.route"
          type="button"
          class="nav-item"
        >
          <i class="bi" :class="item.icon"></i>
          <span>{{ item.title }}</span>
        </component>
      </nav>

      <div class="sidebar-footer">
        <span class="role-chip">{{ roleLabel }}</span>
        <div class="user-name">{{ displayName }}</div>
        <div class="user-sub">Acceso administrativo</div>
      </div>
    </aside>

    <main class="erp-main">
      <header class="topbar">
        <div>
          <div class="eyebrow">Panel de control</div>
          <h1>Centro de operaciones</h1>
          <p>Gestion integral para ventas, inventario y finanzas.</p>
        </div>
        <div class="topbar-actions">
          <div class="search-box">
            <i class="bi bi-search"></i>
            <input type="text" placeholder="Buscar modulo o reporte" />
          </div>
          <div class="action-buttons">
            <button class="icon-btn" type="button">
              <i class="bi bi-bell"></i>
            </button>
            <button class="primary-btn" type="button" @click="logout">
              <i class="bi bi-box-arrow-right"></i>
              Salir
            </button>
          </div>
        </div>
      </header>

      <section class="hero-grid">
        <div class="hero-card">
          <div class="hero-head">
            <div>
              <div class="eyebrow">Ambiente activo</div>
              <h3>{{ activeBranch.name }}</h3>
              <p>Central y Esteli operan sincronizados.</p>
            </div>
            <div v-if="canChooseBranch" class="branch-buttons">
              <button
                v-for="branch in branches"
                :key="branch.code"
                type="button"
                :class="['chip-btn', activeBranch.code === branch.code ? 'active' : '']"
                @click="setBranch(branch)"
              >
                {{ branch.name }}
              </button>
            </div>
            <div v-else class="branch-text">Asignado por rol</div>
          </div>
          <div class="kpi-grid">
            <div class="kpi-card">
              <span>Estado operativo</span>
              <strong>Estable</strong>
            </div>
            <div class="kpi-card">
              <span>Seguridad</span>
              <strong>Control total</strong>
            </div>
            <div class="kpi-card">
              <span>Usuarios activos</span>
              <strong>12</strong>
            </div>
          </div>
        </div>
        <div class="side-card">
          <div class="eyebrow">Actividad reciente</div>
          <ul class="activity-list">
            <li>
              <span class="dot success"></span>
              Cierre de caja completado.
            </li>
            <li>
              <span class="dot primary"></span>
              Inventario sincronizado en Central.
            </li>
            <li>
              <span class="dot warning"></span>
              Reporte financiero listo.
            </li>
          </ul>
        </div>
      </section>

      <section class="modules-header">
        <div>
          <div class="eyebrow">Accesos rapidos</div>
          <h2>Modulos principales</h2>
        </div>
        <button class="ghost-btn" type="button">Ver todo</button>
      </section>

      <section class="module-grid">
        <article class="module-card" v-for="item in menuItems" :key="item.title">
          <div class="module-icon">
            <i class="bi" :class="item.icon"></i>
          </div>
          <div>
            <h4>{{ item.title }}</h4>
            <p>{{ item.description }}</p>
          </div>
          <component
            :is="item.route ? 'router-link' : 'button'"
            :to="item.route"
            class="primary-btn compact"
            type="button"
          >
            Entrar
          </component>
        </article>
      </section>
    </main>
  </div>
</template>

<script>
export default {
  data() {
    return {
      displayName: "Usuario",
      role: null,
      branches: [],
      activeBranch: { code: "", name: "" },
      accessError: null,
      menuItems: [
        {
          title: "Ecommerce",
          description: "Pedidos digitales y control de entregas.",
          icon: "bi-bag-check",
        },
        {
          title: "Finanzas",
          description: "Caja diaria, egresos y resumen de flujo.",
          icon: "bi-cash-stack",
        },
        {
          title: "Inventarios",
          description: "Stock por lote y movimientos por sucursal.",
          icon: "bi-box-seam",
          route: "/inventory",
        },
        {
          title: "Contabilidad",
          description: "Cierres mensuales y reportes contables.",
          icon: "bi-journal-text",
        },
        {
          title: "Gestion de TI",
          description: "Seguridad, roles y soporte operativo.",
          icon: "bi-shield-lock",
        },
      ],
    };
  },
  computed: {
    roleLabel() {
      return this.role === "administrador" ? "Administrador" : "Usuario";
    },
    canChooseBranch() {
      return ["administrador", "seguridad"].includes(this.role);
    },
  },
  async mounted() {
    const token = localStorage.getItem("token");
    if (!token) {
      this.$router.push("/login");
      return;
    }

    try {
      const res = await fetch("http://127.0.0.1:8000/auth/me", {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });

      if (!res.ok) {
        throw new Error("Auth failed");
      }

      const data = await res.json();
      this.displayName = data.full_name || data.email;
      const roleNames = (data.roles || []).map((role) => role.name);
      this.role = roleNames[0] || "usuario";
      this.branches =
        data.branches && data.branches.length
          ? data.branches.map((branch) => ({
              code: branch.code,
              name: branch.name,
            }))
          : [{ code: "central", name: "Central" }];
      this.activeBranch = this.branches[0];

      if (this.role !== "administrador") {
        this.accessError = "Acceso exclusivo para administradores";
        this.logout();
      }
    } catch (error) {
      this.accessError = "Sesion invalida";
      this.logout();
    }
  },
  methods: {
    setBranch(branch) {
      this.activeBranch = branch;
    },
    logout() {
      localStorage.removeItem("token");
      this.$router.push("/login");
    },
  },
};
</script>

<style scoped>
.erp-shell {
  min-height: 100vh;
  display: grid;
  grid-template-columns: 270px 1fr;
  background: #eef2f8;
  color: #0f172a;
}

.erp-sidebar {
  padding: 1.6rem;
  display: flex;
  flex-direction: column;
  gap: 1.6rem;
  background: #ffffff;
  border-right: 1px solid #e3e8f4;
}

.brand {
  display: flex;
  gap: 0.8rem;
  align-items: center;
}

.brand-mark {
  width: 46px;
  height: 46px;
  border-radius: 14px;
  background: linear-gradient(135deg, #0b2a5b, #163a7a);
  color: #fff;
  display: grid;
  place-items: center;
  font-weight: 700;
  letter-spacing: 0.2em;
}

.brand-name {
  font-weight: 600;
}

.brand-sub {
  text-transform: uppercase;
  letter-spacing: 0.2em;
  font-size: 0.7rem;
  color: #5b6b88;
}

.nav-title {
  text-transform: uppercase;
  letter-spacing: 0.22em;
  font-size: 0.7rem;
  color: #5b6b88;
  font-weight: 600;
}

.nav-stack {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.nav-item {
  border: 1px solid #e5eaf6;
  border-radius: 12px;
  padding: 0.6rem 0.8rem;
  background: #f9fbff;
  display: flex;
  align-items: center;
  gap: 0.6rem;
  color: #1f2a44;
  font-weight: 600;
  cursor: pointer;
  transition: 0.2s ease;
}

.nav-item:hover {
  background: #eef3ff;
  border-color: #c8d6f2;
}

.nav-item i {
  color: #0b2a5b;
}

.sidebar-footer {
  margin-top: auto;
  padding-top: 1rem;
  border-top: 1px solid #e3e8f4;
  display: flex;
  flex-direction: column;
  gap: 0.3rem;
}

.role-chip {
  display: inline-flex;
  padding: 0.3rem 0.8rem;
  border-radius: 999px;
  background: #f4f7ff;
  color: #0b2a5b;
  font-size: 0.7rem;
  letter-spacing: 0.2em;
  text-transform: uppercase;
  font-weight: 600;
}

.user-name {
  font-weight: 600;
}

.user-sub {
  color: #7b8aa6;
  font-size: 0.85rem;
}

.erp-main {
  padding: 2.2rem 2.6rem 3rem;
}

.topbar {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
  border-bottom: 1px solid #d8e0f2;
  padding-bottom: 1.4rem;
}

.topbar h1 {
  margin: 0.3rem 0 0.4rem;
  font-size: 2rem;
}

.topbar p {
  margin: 0;
  color: #5b6b88;
}

.eyebrow {
  text-transform: uppercase;
  letter-spacing: 0.22em;
  font-size: 0.7rem;
  color: #6a7ba0;
  font-weight: 600;
}

.topbar-actions {
  display: flex;
  flex-direction: column;
  gap: 0.8rem;
}

.search-box {
  width: min(360px, 100%);
  background: #ffffff;
  border: 1px solid #dbe3f5;
  border-radius: 14px;
  padding: 0.55rem 0.8rem;
  display: flex;
  align-items: center;
  gap: 0.6rem;
  color: #6a7ba0;
}

.search-box input {
  border: none;
  outline: none;
  width: 100%;
  font-size: 0.95rem;
  color: #1f2a44;
  background: transparent;
}

.action-buttons {
  display: flex;
  gap: 0.6rem;
  align-items: center;
}

.icon-btn {
  width: 40px;
  height: 40px;
  border-radius: 12px;
  border: 1px solid #dbe3f5;
  background: #ffffff;
  color: #1f2a44;
  display: grid;
  place-items: center;
  cursor: pointer;
}

.primary-btn {
  border: none;
  background: #0b2a5b;
  color: #ffffff;
  padding: 0.6rem 1.2rem;
  border-radius: 12px;
  font-weight: 600;
  display: inline-flex;
  align-items: center;
  gap: 0.5rem;
  cursor: pointer;
}

.primary-btn.compact {
  padding: 0.5rem 1rem;
}

.hero-grid {
  display: grid;
  grid-template-columns: 1.6fr 1fr;
  gap: 1.2rem;
  margin-top: 1.2rem;
}

.hero-card,
.side-card {
  background: #ffffff;
  border: 1px solid #e0e7f7;
  border-radius: 18px;
  padding: 1.4rem;
  box-shadow: 0 16px 30px rgba(15, 23, 42, 0.06);
}

.hero-head {
  display: flex;
  justify-content: space-between;
  gap: 1rem;
}

.hero-head h3 {
  margin: 0.4rem 0 0.2rem;
}

.hero-head p {
  margin: 0;
  color: #5b6b88;
}

.branch-buttons {
  display: flex;
  gap: 0.5rem;
  flex-wrap: wrap;
}

.chip-btn {
  border: 1px solid #d7e0f2;
  background: #f4f7ff;
  color: #1f2a44;
  padding: 0.3rem 0.7rem;
  border-radius: 999px;
  font-weight: 600;
  cursor: pointer;
}

.chip-btn.active {
  background: #0b2a5b;
  border-color: #0b2a5b;
  color: #ffffff;
}

.branch-text {
  color: #7b8aa6;
  font-size: 0.85rem;
  margin-top: 0.5rem;
}

.kpi-grid {
  margin-top: 1rem;
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 0.8rem;
}

.kpi-card {
  border-radius: 14px;
  padding: 0.9rem 1rem;
  background: #f7f9ff;
  border: 1px solid #dfe7f6;
  display: flex;
  flex-direction: column;
  gap: 0.4rem;
  color: #1f2a44;
}

.kpi-card span {
  color: #6a7ba0;
  font-size: 0.85rem;
}

.kpi-card strong {
  font-size: 1.05rem;
}

.activity-list {
  display: grid;
  gap: 0.9rem;
  color: #5b6b88;
  margin-top: 1rem;
}

.activity-list li {
  display: flex;
  align-items: center;
  gap: 0.6rem;
}

.dot {
  width: 10px;
  height: 10px;
  border-radius: 999px;
  display: inline-block;
}

.dot.success {
  background: #2fbf71;
}

.dot.primary {
  background: #3b5bdb;
}

.dot.warning {
  background: #f5a524;
}

.modules-header {
  margin-top: 1.6rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.modules-header h2 {
  margin: 0.4rem 0 0;
  font-size: 1.25rem;
}

.ghost-btn {
  border: 1px solid #d7e0f2;
  background: transparent;
  color: #1f2a44;
  padding: 0.4rem 0.9rem;
  border-radius: 10px;
  font-weight: 600;
  cursor: pointer;
}

.module-grid {
  margin-top: 1rem;
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
  gap: 1rem;
}

.module-card {
  background: #ffffff;
  border: 1px solid #e0e7f7;
  border-radius: 18px;
  padding: 1.2rem;
  display: flex;
  flex-direction: column;
  gap: 0.9rem;
  box-shadow: 0 12px 24px rgba(15, 23, 42, 0.06);
}

.module-card h4 {
  margin: 0 0 0.4rem;
}

.module-card p {
  margin: 0;
  color: #5b6b88;
}

.module-icon {
  width: 46px;
  height: 46px;
  border-radius: 14px;
  background: rgba(11, 42, 91, 0.12);
  color: #0b2a5b;
  display: grid;
  place-items: center;
  font-size: 1.2rem;
}

@media (max-width: 992px) {
  .erp-shell {
    grid-template-columns: 1fr;
  }

  .erp-sidebar {
    flex-direction: row;
    align-items: center;
    overflow-x: auto;
  }

  .hero-grid {
    grid-template-columns: 1fr;
  }
}

@media (min-width: 992px) {
  .topbar {
    flex-direction: row;
    align-items: center;
    justify-content: space-between;
  }

  .topbar-actions {
    flex-direction: row;
    align-items: center;
  }
}
</style>
