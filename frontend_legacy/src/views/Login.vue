<template>
  <div class="login-wrap">
    <div class="login-grid">
      <section class="login-hero">
        <div class="hero-brand">
          <div class="brand-mark">HP</div>
          <div>
            <div class="fw-semibold">Hollywood Pacas</div>
            <div class="text-uppercase small text-muted">ERP Comercial</div>
          </div>
        </div>

        <h1 class="display-6 fw-semibold mt-4 phrase">
          {{ currentPhrase }}
        </h1>
        <p class="text-muted mt-3">
          Controla ventas, inventario y finanzas con una experiencia clara y
          profesional en cada sucursal.
        </p>

        <div class="video-wrap mt-4">
          <video
            class="hero-video"
            src="../assets/main_video.mp4"
            autoplay
            loop
            muted
            playsinline
          ></video>
        </div>
      </section>

      <section class="login-panel">
        <div class="login-panel-inner">
          <form class="card shadow-sm border-0" @submit.prevent="login">
            <div class="card-body p-4 p-md-4">
            <div class="text-center">
              <div class="logo-area mx-auto mb-2">
                <img
                  src="../assets/logo_hollywood.png"
                  alt="Hollywood Pacas"
                  class="logo-img"
                />
              </div>
              <div class="text-uppercase small text-primary">Acceso seguro</div>
              <h2 class="h4 fw-semibold mt-2">Inicia sesion</h2>
              <p class="text-muted small">
                Bienvenido de nuevo. Ingresa con tus credenciales.
              </p>
            </div>

            <div class="mt-2">
              <label class="form-label fw-semibold">Usuario o correo</label>
              <input
                v-model="email"
                type="text"
                class="form-control form-control-lg"
                autocomplete="username"
                placeholder="Usuario"
              />
            </div>

            <div class="mt-2">
              <label class="form-label fw-semibold">Contrasena</label>
              <input
                v-model="password"
                type="password"
                class="form-control form-control-lg"
                autocomplete="current-password"
                placeholder="Contrasena"
              />
            </div>

            <div class="d-flex justify-content-between align-items-center mt-2">
              <div class="form-check">
                <input
                  v-model="remember"
                  class="form-check-input"
                  type="checkbox"
                  id="rememberMe"
                />
                <label class="form-check-label" for="rememberMe">
                  Mantener sesion
                </label>
              </div>
              <button type="button" class="btn btn-link p-0 text-decoration-none">
                Soporte TI
              </button>
            </div>

            <button class="btn btn-primary btn-lg w-100 mt-2" type="submit">
              Ingresar
            </button>

            <p v-if="error" class="text-danger text-center mt-2">
              {{ error }}
            </p>
            </div>
          </form>

          <div class="login-footer mt-3">
            <span>Copyright 2025 - Autorizado a Hollywood Pacas</span>
            <div class="footer-links">
              <a href="#" class="text-decoration-none">Licencia</a>
              <a href="#" class="text-decoration-none">Uso de software</a>
              <a href="#" class="text-decoration-none">Soporte</a>
            </div>
          </div>
        </div>
      </section>
    </div>
  </div>
</template>

<script>
export default {
  data() {
    return {
      email: "",
      password: "",
      remember: true,
      error: null,
      phraseIndex: 0,
      phrases: [
        "Un ERP elegante para un comercio moderno.",
        "Tecnologia clara para decisiones rapidas.",
        "Software que ordena y acelera tu negocio.",
        "Procesos simples, resultados grandes.",
        "Datos confiables para crecer con seguridad.",
        "Innovacion que se siente en cada venta.",
        "Control total sin complicaciones.",
        "Visibilidad en tiempo real para cada area.",
        "Productividad que eleva a tu equipo.",
        "Operacion estable con soporte inteligente.",
        "Gestion profesional para empresas exigentes.",
        "Calidad que se nota en cada reporte.",
        "Flujo de trabajo limpio y eficiente.",
        "Seguridad y orden para escalar rapido.",
        "Menos friccion, mas rendimiento.",
        "Finanzas claras, inventario preciso.",
        "Un sistema que acompana tu crecimiento.",
        "Tecnologia confiable para liderar.",
        "Procesos conectados, equipos enfocados.",
        "Eficiencia diaria con estilo profesional.",
        "Decisiones estrategicas con datos vivos.",
        "Agilidad para competir y ganar.",
        "Automatiza lo repetitivo, enfoca lo importante.",
        "Control y elegancia en un solo lugar.",
        "Tu empresa, organizada y lista para el futuro.",
      ],
    };
  },
  computed: {
    currentPhrase() {
      return this.phrases[this.phraseIndex];
    },
  },
  mounted() {
    this.phraseTimer = setInterval(() => {
      this.phraseIndex = (this.phraseIndex + 1) % this.phrases.length;
    }, 5000);
  },
  beforeUnmount() {
    if (this.phraseTimer) {
      clearInterval(this.phraseTimer);
    }
  },
  methods: {
    async login() {
      try {
        const res = await fetch("http://127.0.0.1:8000/auth/login", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            email: this.email,
            password: this.password,
          }),
        });

        if (!res.ok) {
          this.error = "Credenciales incorrectas";
          return;
        }

        const data = await res.json();
        localStorage.setItem("token", data.access_token);

        this.$router.push("/home");
      } catch (err) {
        this.error = "Error de conexion";
      }
    },
  },
};
</script>

<style scoped>
.login-wrap {
  min-height: 100vh;
  background: linear-gradient(120deg, #0f1f3d 0%, #f5f8ff 65%);
  overflow: hidden;
}

.login-grid {
  height: 100vh;
  display: grid;
  grid-template-columns: 1.1fr 0.9fr;
}

.login-hero {
  padding: 3rem 3.5rem;
  color: #f5f8ff;
  position: relative;
}

.login-panel {
  padding: 2rem 2.5rem;
  background: #ffffff;
  border-left: 1px solid #dbe3f5;
  display: flex;
  align-items: center;
  justify-content: center;
}

.login-panel-inner {
  width: 100%;
  max-width: 520px;
}

.hero-brand {
  display: flex;
  align-items: center;
  gap: 1rem;
}

.phrase {
  min-height: 3.5rem;
  animation: phraseFade 0.5s ease;
}

.brand-mark {
  width: 48px;
  height: 48px;
  border-radius: 14px;
  background: linear-gradient(135deg, #1b3b8f, #0f2b6b);
  color: #fff;
  display: grid;
  place-items: center;
  font-weight: 700;
  letter-spacing: 0.2em;
}

.soft-card {
  border: 1px solid #dbe3f5;
  background: #ffffff;
  box-shadow: 0 10px 24px rgba(17, 24, 39, 0.08);
  color: #101828;
}

.video-wrap {
  display: flex;
  justify-content: center;
}

.hero-video {
  width: min(640px, 100%);
  border-radius: 24px;
  box-shadow: 0 20px 44px rgba(10, 20, 40, 0.4);
}

.logo-area {
  width: 240px;
  height: 240px;
  background: #ffffff;
  border-radius: 24px;
  display: grid;
  place-items: center;
}

.logo-img {
  max-width: 92%;
  max-height: 92%;
  object-fit: contain;
}

.login-hero :deep(.text-muted) {
  color: rgba(245, 248, 255, 0.75) !important;
}

.login-wrap :deep(.text-primary) {
  color: #1b3b8f !important;
}

.login-panel :deep(.btn-primary) {
  background-color: #0f2b6b;
  border-color: #0f2b6b;
}

.login-hero::before,
.login-hero::after {
  content: "";
  position: absolute;
  border-radius: 999px;
  filter: blur(60px);
  opacity: 0.6;
  z-index: 0;
}

.login-hero::before {
  width: 220px;
  height: 220px;
  top: 10%;
  left: -80px;
  background: rgba(120, 160, 255, 0.45);
}

.login-hero::after {
  width: 260px;
  height: 260px;
  bottom: 8%;
  right: -120px;
  background: rgba(200, 170, 255, 0.35);
}

.login-hero > * {
  position: relative;
  z-index: 1;
}

.login-footer {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  text-align: center;
  color: #7b8aa6;
  font-size: 0.85rem;
}

.footer-links {
  display: flex;
  justify-content: center;
  gap: 1rem;
}

@keyframes phraseFade {
  from {
    opacity: 0;
    transform: translateY(6px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

@media (max-width: 992px) {
  .login-grid {
    grid-template-columns: 1fr;
    height: auto;
  }

  .login-panel {
    border-left: none;
    border-top: 1px solid #eef2ff;
  }
}
</style>
