<template>
  <div class="layout">

    <!-- SIDEBAR -->
    <aside :class="['sidebar shadow-sm', collapsed ? 'collapsed' : '']">

      <div class="sidebar-header d-flex align-items-center justify-content-between">
        <h3 v-if="!collapsed" class="brand-title">Orange Tec</h3>

        <button class="btn-toggle" @click="toggleSidebar">
          <i class="bi" :class="collapsed ? 'bi-chevron-double-right' : 'bi-chevron-double-left'"></i>
        </button>
      </div>

      <hr class="divider" />

      <!-- MENU -->
      <nav class="menu">

        <a class="menu-item"
           href="#"
           data-bs-toggle="tooltip"
           :title="collapsed ? 'Dashboard' : ''">
          <i class="bi bi-speedometer2"></i>
          <span v-if="!collapsed">Dashboard</span>
        </a>

        <a class="menu-item"
           href="#"
           data-bs-toggle="tooltip"
           :title="collapsed ? 'Usuarios' : ''">
          <i class="bi bi-people-fill"></i>
          <span v-if="!collapsed">Usuarios</span>
        </a>

        <a class="menu-item"
           href="#"
           data-bs-toggle="tooltip"
           :title="collapsed ? 'Productos' : ''">
          <i class="bi bi-box-seam"></i>
          <span v-if="!collapsed">Productos</span>
        </a>

        <a class="menu-item"
           href="#"
           data-bs-toggle="tooltip"
           :title="collapsed ? 'Inventario' : ''">
          <i class="bi bi-archive"></i>
          <span v-if="!collapsed">Inventario</span>
        </a>

        <a class="menu-item"
           href="#"
           data-bs-toggle="tooltip"
           :title="collapsed ? 'Ventas' : ''">
          <i class="bi bi-cart-check"></i>
          <span v-if="!collapsed">Ventas</span>
        </a>

      </nav>
    </aside>

    <!-- MAIN CONTENT -->
    <main class="content">

      <!-- NAVBAR SUPERIOR -->
      <nav class="topbar bg-white shadow-sm px-4">
        
        <div class="d-flex align-items-center">
          <h4 class="fw-semibold text-dark mb-0">Panel de Control</h4>
        </div>

        <div class="d-flex align-items-center gap-3">
          <!-- MENU DE USUARIO -->
          <div class="dropdown">
            <button class="btn btn-light border dropdown-toggle" 
                    data-bs-toggle="dropdown">
              <i class="bi bi-person-circle me-2"></i> Administrador
            </button>

            <ul class="dropdown-menu dropdown-menu-end">
              <li><a class="dropdown-item" href="#"><i class="bi bi-person"></i> Perfil</a></li>
              <li><a class="dropdown-item" href="#"><i class="bi bi-gear"></i> Configuración</a></li>
              <li><hr class="dropdown-divider"></li>
              <li>
                <a class="dropdown-item text-danger" @click="logout">
                  <i class="bi bi-box-arrow-right"></i> Cerrar Sesión
                </a>
              </li>
            </ul>
          </div>
        </div>

      </nav>

      <!-- BREADCRUMB -->
      <div class="px-4 pt-3">
        <nav aria-label="breadcrumb">
          <ol class="breadcrumb">
            <li class="breadcrumb-item"><a href="#">Inicio</a></li>
            <li class="breadcrumb-item active">Dashboard</li>
          </ol>
        </nav>
      </div>

      <!-- KPIs -->
      <div class="container-fluid px-4 mt-4">
        <div class="row g-4">

          <div class="col-md-4">
            <div class="kpi-card gradient-purple shadow-sm">
              <div>
                <h6 class="kpi-label">Ventas Hoy</h6>
                <h2 class="kpi-value">C$ 12,450</h2>
              </div>
              <i class="bi bi-bar-chart-fill kpi-icon"></i>
            </div>
          </div>

          <div class="col-md-4">
            <div class="kpi-card gradient-green shadow-sm">
              <div>
                <h6 class="kpi-label">Inventario</h6>
                <h2 class="kpi-value">842 Items</h2>
              </div>
              <i class="bi bi-boxes kpi-icon"></i>
            </div>
          </div>

          <div class="col-md-4">
            <div class="kpi-card gradient-blue shadow-sm">
              <div>
                <h6 class="kpi-label">Usuarios Activos</h6>
                <h2 class="kpi-value">12</h2>
              </div>
              <i class="bi bi-people-fill kpi-icon"></i>
            </div>
          </div>
        </div>
      </div>

      <!-- TOASTs -->
      <div class="position-fixed bottom-0 end-0 p-3" style="z-index: 9999">
        <div id="toastSuccess" 
             class="toast text-bg-success border-0" 
             role="alert">
          <div class="toast-body">
            Acción realizada correctamente.
          </div>
        </div>
      </div>

    </main>
  </div>
</template>

<script>
export default {
  data() {
    return {
      collapsed: false,
    };
  },

  mounted() {
    // Activar tooltips (solo funcionan después de cargar la vista)
    const tooltips = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    tooltips.forEach(t => new bootstrap.Tooltip(t));
  },

  methods: {
    toggleSidebar() {
      this.collapsed = !this.collapsed;

      // Re-inicializar tooltips cuando se colapsa
      setTimeout(() => {
        const tooltips = document.querySelectorAll('[data-bs-toggle="tooltip"]');
        tooltips.forEach(t => new bootstrap.Tooltip(t));
      }, 200);
    },

    showToast() {
      const toastEl = document.getElementById("toastSuccess");
      const toast = new bootstrap.Toast(toastEl);
      toast.show();
    },

    logout() {
      localStorage.removeItem("token");
      this.$router.push("/login");
    },
  },
};
</script>

<style scoped>
/* GLOBAL FONT */
*{
  font-family: 'Poppins', sans-serif;
}

/* LAYOUT */
.layout{
  display: flex;
  min-height: 100vh;
  background: #f7f7fb;
}

/* SIDEBAR */
.sidebar{
  width: 260px;
  background: white;
  padding: 15px 12px;
  border-right: 1px solid #e3e3e3;
  transition: width 0.25s ease;
}
.sidebar.collapsed{
  width: 90px;
}

.brand-title{
  color: #7b2cbf;
}

/* BTN TOGGLE */
.btn-toggle{
  background: transparent;
  border: none;
  font-size: 1.4rem;
  color: #7b2cbf;
  transition: 0.2s;
}
.btn-toggle:hover{
  color: #9d4edd;
}

/* MENU */
.menu{
  display: flex;
  flex-direction: column;
  margin-top: 15px;
  gap: 8px;
}

.menu-item{
  padding: 12px 14px;
  border-radius: 8px;
  font-weight: 500;
  display: flex;
  align-items: center;
  gap: 12px;
  color: #444;
  transition: 0.25s;
}

.menu-item i{
  font-size: 1.3rem;
  color: #7b2cbf;
}

.menu-item:hover{
  background: #f3e8ff;
  color: #7b2cbf;
  transform: translateX(6px);
  font-weight: 600;
}

/* CONTENT */
.content{
  flex-grow: 1;
}

/* TOPBAR */
.topbar{
  height: 70px;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

/* KPI CARDS */
.kpi-card{
  padding: 25px;
  border-radius: 16px;
  color: white;
  display: flex;
  justify-content: space-between;
  align-items: center;
  transition: 0.25s;
}
.kpi-card:hover{
  transform: translateY(-6px);
  cursor: pointer;
}

.gradient-purple{
  background: linear-gradient(135deg, #7b2cbf, #9d4edd);
}
.gradient-green{
  background: linear-gradient(135deg, #2ea44f, #47d864);
}
.gradient-blue{
  background: linear-gradient(135deg, #266dd3, #4ea8de);
}

.kpi-icon{
  font-size: 3rem;
  opacity: 0.25;
}

.kpi-label{
  font-size: 0.9rem;
  opacity: 0.85;
  text-transform: uppercase;
}
.kpi-value{
  font-size: 2rem;
  font-weight: 800;
}

</style>
