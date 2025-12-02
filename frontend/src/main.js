import { createApp } from 'vue'
import App from './App.vue'
import router from "./router";



// Bootstrap
import "bootstrap/dist/css/bootstrap.min.css";
import "bootstrap";
import "bootstrap/dist/js/bootstrap.bundle.min.js"
import "bootstrap-icons/font/bootstrap-icons.css"

// HTMX
import htmx from "htmx.org";
window.htmx = htmx;

createApp(App).use(router).mount("#app");
