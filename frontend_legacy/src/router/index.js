import { createRouter, createWebHistory } from "vue-router";
import Login from "../views/Login.vue";
import Home from "../views/Home.vue";
import Inventory from "../views/Inventory.vue";

const routes = [
  { path: "/", redirect: "/login" },
  { path: "/login", component: Login },
  { path: "/home", component: Home },
  { path: "/inventory", component: Inventory },
];

const router = createRouter({
  history: createWebHistory(),
  routes,
});

export default router;
