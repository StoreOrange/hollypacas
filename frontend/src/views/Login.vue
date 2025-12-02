<template>
<div class="container d-flex justify-content-center align-items-center" style="height: 100vh;">
    <div class="card shadow p-4" style="width: 400px;">
        <h3 class="text-center mb-3">ERP Login</h3>

        <div class="mb-3">
            <label>Email</label>
            <input v-model="email" class="form-control" placeholder="admin@erp.com" />
        </div>

        <div class="mb-3">
            <label>Contraseña</label>
            <input v-model="password" type="password" class="form-control" placeholder="1234" />
        </div>

        <button class="btn btn-primary w-100" @click="login">Ingresar</button>

        <p v-if="error" class="text-danger text-center mt-2">{{ error }}</p>
    </div>
</div>
</template>

<script>
export default {
    data() {
        return {
            email: "admin@erp.com",
            password: "1234",
            error: null,
        };
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
                this.error = "Error de conexión";
            }
        }

    },
};
</script>
