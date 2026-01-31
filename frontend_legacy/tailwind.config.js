/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{vue,js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#161c24",
        slate: "#4b5563",
        sand: "#f4efe8",
        brand: "#c96a2d",
        brandDeep: "#8a4520",
        accent: "#1f7f7a",
      },
      boxShadow: {
        soft: "0 18px 40px rgba(22, 28, 36, 0.12)",
        lift: "0 12px 28px rgba(22, 28, 36, 0.16)",
      },
      fontFamily: {
        heading: ["Fraunces", "serif"],
        body: ["Manrope", "sans-serif"],
      },
    },
  },
  plugins: [],
};
