<template>
  <div class="inventory-shell">
    <header class="page-header">
      <div>
        <div class="eyebrow">Inventario</div>
        <h1>Catalogo de productos</h1>
        <p>Administra productos, precios, costos y existencia.</p>
      </div>
      <div class="header-actions">
        <div class="status-chip">
          {{ isEditing ? "Editando producto" : "Nuevo producto" }}
        </div>
        <button class="ghost-btn" type="button" @click="resetForm">
          Limpiar
        </button>
        <button class="primary-btn" type="button" @click="saveProduct">
          {{ isEditing ? "Actualizar" : "Crear" }}
        </button>
      </div>
    </header>

    <section class="content-grid">
      <form class="product-form" @submit.prevent="saveProduct">
        <div class="form-card">
          <div class="card-title">
            <h2>Datos del producto</h2>
            <span class="helper">Campos obligatorios *</span>
          </div>
          <div class="form-grid">
            <label>
              Codigo *
              <input v-model="form.cod_producto" type="text" required />
            </label>
            <label>
              Descripcion *
              <input v-model="form.descripcion" type="text" required />
            </label>
            <label>
              Linea
              <select v-model="form.linea_id">
                <option value="">Selecciona</option>
                <option
                  v-for="linea in lineas"
                  :key="linea.cod_linea"
                  :value="linea.id"
                >
                  {{ linea.linea }}
                </option>
              </select>
              <button
                class="link-btn"
                type="button"
                @click="showLineaForm = !showLineaForm"
              >
                {{ showLineaForm ? "Cerrar" : "+ Nueva linea" }}
              </button>
            </label>
            <label>
              Segmento
              <select v-model="form.segmento_id">
                <option value="">Selecciona</option>
                <option v-for="seg in segmentos" :key="seg.segmento" :value="seg.id">
                  {{ seg.segmento }}
                </option>
              </select>
              <button
                class="link-btn"
                type="button"
                @click="showSegmentoForm = !showSegmentoForm"
              >
                {{ showSegmentoForm ? "Cerrar" : "+ Nuevo segmento" }}
              </button>
            </label>
            <label>
              Marca
              <input v-model="form.marca" type="text" />
            </label>
            <label>
              Referencia
              <input v-model="form.referencia_producto" type="text" />
            </label>
          </div>
        </div>

        <div class="form-card">
          <div class="card-title">
            <h2>Precios y costos</h2>
            <span class="helper">Moneda: Cordobas</span>
          </div>
          <div class="form-grid">
            <label>
              Precio venta 1
              <input v-model.number="form.precio_venta1" type="number" step="0.01" />
            </label>
            <label>
              Precio venta 2
              <input v-model.number="form.precio_venta2" type="number" step="0.01" />
            </label>
            <label>
              Precio venta 3
              <input v-model.number="form.precio_venta3" type="number" step="0.01" />
            </label>
            <label>
              Costo producto
              <input v-model.number="form.costo_producto" type="number" step="0.01" />
            </label>
            <label>
              Existencia
              <input v-model.number="form.existencia" type="number" step="0.01" />
            </label>
            <label class="switch-label">
              Activo
              <input v-model="form.activo" type="checkbox" />
            </label>
          </div>
        </div>
      </form>

      <section class="table-card">
        <div class="table-header">
          <h2>Listado de productos</h2>
          <div class="table-tools">
            <label class="toggle">
              <input v-model="includeInactive" type="checkbox" @change="fetchProducts" />
              Incluir inactivos
            </label>
            <div class="search-mini">
              <i class="bi bi-search"></i>
              <input v-model="search" type="text" placeholder="Buscar producto" />
            </div>
          </div>
        </div>

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Codigo</th>
                <th>Descripcion</th>
                <th>Linea</th>
                <th>Segmento</th>
                <th>Marca</th>
                <th>Precio 1</th>
                <th>Costo</th>
                <th>Activo</th>
                <th>Registro</th>
                <th>Ultima</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="producto in filteredProducts" :key="producto.id">
                <td>{{ producto.cod_producto }}</td>
                <td>{{ producto.descripcion }}</td>
                <td>{{ producto.linea?.linea || "-" }}</td>
                <td>{{ producto.segmento?.segmento || "-" }}</td>
                <td>{{ producto.marca || "-" }}</td>
                <td>{{ formatMoney(producto.precio_venta1) }}</td>
                <td>{{ formatMoney(producto.costo_producto) }}</td>
                <td>
                  <span :class="['status', producto.activo ? 'ok' : 'off']">
                    {{ producto.activo ? "SI" : "NO" }}
                  </span>
                </td>
                <td>{{ formatDate(producto.registro) }}</td>
                <td>{{ formatDate(producto.ultima_modificacion) }}</td>
                <td class="actions">
                  <button class="ghost-btn" type="button" @click="editProduct(producto)">
                    Editar
                  </button>
                  <button
                    class="danger-btn"
                    type="button"
                    @click="deactivateProduct(producto)"
                  >
                    Desactivar
                  </button>
                </td>
              </tr>
              <tr v-if="!products.length">
                <td colspan="11" class="empty">Sin productos registrados</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>
    </section>

    <section class="catalog-grid" v-if="showLineaForm || showSegmentoForm">
      <div v-if="showLineaForm" class="form-card">
        <h2>Nueva linea</h2>
        <div class="form-grid">
          <label>
            Codigo linea
            <input v-model="lineaForm.cod_linea" type="text" />
          </label>
          <label>
            Nombre linea
            <input v-model="lineaForm.linea" type="text" />
          </label>
          <label class="switch-label">
            Activo
            <input v-model="lineaForm.activo" type="checkbox" />
          </label>
        </div>
        <div class="form-actions">
          <button class="ghost-btn" type="button" @click="resetLineaForm">
            Limpiar
          </button>
          <button class="primary-btn" type="button" @click="saveLinea">
            Guardar linea
          </button>
        </div>
      </div>

      <div v-if="showSegmentoForm" class="form-card">
        <h2>Nuevo segmento</h2>
        <div class="form-grid">
          <label>
            Segmento
            <input v-model="segmentoForm.segmento" type="text" />
          </label>
        </div>
        <div class="form-actions">
          <button class="ghost-btn" type="button" @click="resetSegmentoForm">
            Limpiar
          </button>
          <button class="primary-btn" type="button" @click="saveSegmento">
            Guardar segmento
          </button>
        </div>
      </div>
    </section>
  </div>
</template>

<script>
export default {
  data() {
    return {
      products: [],
      lineas: [],
      segmentos: [],
      includeInactive: false,
      isEditing: false,
      editingId: null,
      search: "",
      showLineaForm: false,
      showSegmentoForm: false,
      lineaForm: {
        cod_linea: "",
        linea: "",
        activo: true,
      },
      segmentoForm: {
        segmento: "",
      },
      form: {
        cod_producto: "",
        descripcion: "",
        linea_id: "",
        segmento_id: "",
        marca: "",
        referencia_producto: "",
        precio_venta1: 0,
        precio_venta2: 0,
        precio_venta3: 0,
        costo_producto: 0,
        existencia: 0,
        activo: true,
      },
    };
  },
  async mounted() {
    await this.fetchCatalogs();
    await this.fetchProducts();
  },
  computed: {
    filteredProducts() {
      const query = this.search.trim().toLowerCase();
      if (!query) return this.products;
      return this.products.filter((producto) => {
        const values = [
          producto.cod_producto,
          producto.descripcion,
          producto.linea?.linea,
          producto.segmento?.segmento,
          producto.marca,
        ]
          .filter(Boolean)
          .join(" ")
          .toLowerCase();
        return values.includes(query);
      });
    },
  },
  methods: {
    async fetchCatalogs() {
      const token = localStorage.getItem("token");
      const res = await fetch("http://127.0.0.1:8000/inventory/catalogs", {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (res.ok) {
        const data = await res.json();
        this.lineas = data.lineas || [];
        this.segmentos = data.segmentos || [];
      }
    },
    async fetchProducts() {
      const token = localStorage.getItem("token");
      const res = await fetch(
        `http://127.0.0.1:8000/inventory/products?include_inactive=${this.includeInactive}`,
        { headers: { Authorization: `Bearer ${token}` } }
      );
      if (res.ok) {
        this.products = await res.json();
      }
    },
    async saveProduct() {
      const token = localStorage.getItem("token");
      const payload = { ...this.form };
      payload.linea_id = payload.linea_id || null;
      payload.segmento_id = payload.segmento_id || null;

      const url = this.isEditing
        ? `http://127.0.0.1:8000/inventory/products/${this.editingId}`
        : "http://127.0.0.1:8000/inventory/products";
      const method = this.isEditing ? "PUT" : "POST";
      if (this.isEditing) {
        delete payload.cod_producto;
      }

      const res = await fetch(url, {
        method,
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify(payload),
      });

      if (res.ok) {
        await this.fetchProducts();
        this.resetForm();
      }
    },
    editProduct(producto) {
      this.isEditing = true;
      this.editingId = producto.id;
      this.form = {
        cod_producto: producto.cod_producto,
        descripcion: producto.descripcion,
        linea_id: producto.linea?.id || "",
        segmento_id: producto.segmento?.id || "",
        marca: producto.marca || "",
        referencia_producto: producto.referencia_producto || "",
        precio_venta1: Number(producto.precio_venta1 || 0),
        precio_venta2: Number(producto.precio_venta2 || 0),
        precio_venta3: Number(producto.precio_venta3 || 0),
        costo_producto: Number(producto.costo_producto || 0),
        existencia: Number(producto.saldo?.existencia || 0),
        activo: producto.activo,
      };
    },
    async deactivateProduct(producto) {
      const token = localStorage.getItem("token");
      await fetch(
        `http://127.0.0.1:8000/inventory/products/${producto.id}/deactivate`,
        { method: "PATCH", headers: { Authorization: `Bearer ${token}` } }
      );
      await this.fetchProducts();
    },
    resetForm() {
      this.isEditing = false;
      this.editingId = null;
      this.form = {
        cod_producto: "",
        descripcion: "",
        linea_id: "",
        segmento_id: "",
        marca: "",
        referencia_producto: "",
        precio_venta1: 0,
        precio_venta2: 0,
        precio_venta3: 0,
        costo_producto: 0,
        existencia: 0,
        activo: true,
      };
    },
    async saveLinea() {
      const token = localStorage.getItem("token");
      const res = await fetch("http://127.0.0.1:8000/inventory/lineas", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify(this.lineaForm),
      });
      if (res.ok) {
        await this.fetchCatalogs();
        this.resetLineaForm();
      }
    },
    async saveSegmento() {
      const token = localStorage.getItem("token");
      const res = await fetch("http://127.0.0.1:8000/inventory/segmentos", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify(this.segmentoForm),
      });
      if (res.ok) {
        await this.fetchCatalogs();
        this.resetSegmentoForm();
      }
    },
    resetLineaForm() {
      this.lineaForm = { cod_linea: "", linea: "", activo: true };
    },
    resetSegmentoForm() {
      this.segmentoForm = { segmento: "" };
    },
    formatMoney(value) {
      const num = Number(value || 0);
      return `C$ ${num.toFixed(2)}`;
    },
    formatDate(value) {
      if (!value) return "-";
      const date = new Date(value);
      return date.toLocaleDateString();
    },
  },
};
</script>

<style scoped>
.inventory-shell {
  padding: 2rem 2.6rem 3rem;
  color: #0f172a;
}

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 1rem;
  border-bottom: 1px solid #d8e0f2;
  padding-bottom: 1.2rem;
}

.page-header h1 {
  margin: 0.3rem 0 0.4rem;
}

.page-header p {
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

.header-actions {
  display: flex;
  gap: 0.6rem;
  align-items: center;
}

.status-chip {
  padding: 0.35rem 0.75rem;
  border-radius: 999px;
  background: #eef3ff;
  color: #0b2a5b;
  font-size: 0.8rem;
  font-weight: 600;
}

.content-grid {
  margin-top: 1.6rem;
  display: grid;
  grid-template-columns: minmax(320px, 420px) 1fr;
  gap: 1.4rem;
}

.form-card {
  background: #ffffff;
  border: 1px solid #e0e7f7;
  border-radius: 18px;
  padding: 1.2rem;
  box-shadow: 0 12px 24px rgba(15, 23, 42, 0.06);
  margin-bottom: 1rem;
}

.form-card h2 {
  margin: 0 0 1rem;
  font-size: 1rem;
}

.card-title {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 0.8rem;
}

.card-title h2 {
  margin: 0;
}

.helper {
  color: #7b8aa6;
  font-size: 0.8rem;
}

.form-grid {
  display: grid;
  gap: 0.8rem;
}

.form-grid label {
  font-size: 0.85rem;
  font-weight: 600;
  color: #1f2a44;
  display: grid;
  gap: 0.4rem;
}

.form-grid input,
.form-grid select {
  border: 1px solid #dbe3f5;
  border-radius: 10px;
  padding: 0.5rem 0.6rem;
  font-size: 0.9rem;
}

.form-grid input:focus,
.form-grid select:focus {
  outline: none;
  border-color: #9fb2da;
  box-shadow: 0 0 0 3px rgba(15, 42, 91, 0.12);
}

.link-btn {
  margin-top: 0.4rem;
  background: transparent;
  border: none;
  color: #0b2a5b;
  font-size: 0.8rem;
  font-weight: 600;
  text-align: left;
  cursor: pointer;
}

.form-actions {
  display: flex;
  gap: 0.6rem;
  margin-top: 0.8rem;
}

.switch-label {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.table-card {
  background: #ffffff;
  border: 1px solid #e0e7f7;
  border-radius: 18px;
  padding: 1.2rem;
  box-shadow: 0 12px 24px rgba(15, 23, 42, 0.06);
}

.catalog-grid {
  margin-top: 1.6rem;
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
  gap: 1rem;
}

.table-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 0.8rem;
  gap: 0.8rem;
}

.table-header h2 {
  margin: 0;
  font-size: 1rem;
}

.table-tools {
  display: flex;
  align-items: center;
  gap: 0.8rem;
}

.search-mini {
  display: flex;
  align-items: center;
  gap: 0.4rem;
  border: 1px solid #dbe3f5;
  border-radius: 10px;
  padding: 0.35rem 0.6rem;
  color: #6a7ba0;
}

.search-mini input {
  border: none;
  outline: none;
  background: transparent;
  font-size: 0.85rem;
  width: 160px;
}

.table-wrap {
  overflow: auto;
  max-height: 540px;
}

table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.85rem;
}

tbody tr:hover {
  background: #f5f8ff;
}

th,
td {
  padding: 0.6rem 0.5rem;
  border-bottom: 1px solid #eef2fb;
  text-align: left;
  white-space: nowrap;
}

thead th {
  color: #6a7ba0;
  font-weight: 600;
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  position: sticky;
  top: 0;
  background: #ffffff;
  z-index: 1;
}

.status {
  padding: 0.2rem 0.5rem;
  border-radius: 999px;
  font-size: 0.75rem;
  font-weight: 600;
}

.status.ok {
  background: #e7f6ed;
  color: #207a3a;
}

.status.off {
  background: #fce8e8;
  color: #b42318;
}

.actions {
  display: flex;
  gap: 0.4rem;
}

.empty {
  text-align: center;
  color: #7b8aa6;
  padding: 1rem;
}

.toggle {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.85rem;
  color: #5b6b88;
}

.primary-btn,
.ghost-btn,
.danger-btn {
  border: none;
  border-radius: 10px;
  padding: 0.5rem 1rem;
  font-weight: 600;
  cursor: pointer;
}

.primary-btn {
  background: #0b2a5b;
  color: #ffffff;
}

.ghost-btn {
  background: #f4f7ff;
  color: #0b2a5b;
  border: 1px solid #d7e0f2;
}

.danger-btn {
  background: #fce8e8;
  color: #b42318;
}

@media (max-width: 1100px) {
  .content-grid {
    grid-template-columns: 1fr;
  }
}
</style>
