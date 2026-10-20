const mongoose=require('mongoose')
const productSchema=new mongoose.Schema({
    name: {
        type: String,
        required: true,
        trim: true,
        minlength: 2,
        maxlength: 120,
    },

    sku: {
        type: String,
        required: true,
        unique: true,
        trim: true,
        uppercase: true,
        minlength: 3,
        maxlength: 40,
        match: /^[A-Z0-9_-]+$/,
    },

    description: {
        type: String,
        trim: true,
        maxlength: 1000,
        default: "",
    },

    price: {
        type: Number,
        required: true,
        min: 0,
    },

    availableStock: {
        type: Number,
        required: true,
        default: 0,
        min: 0,
        validate: Number.isSafeInteger,
    },

    reservedStock: {
        type: Number,
        default: 0,
        min: 0,
        validate: Number.isSafeInteger,
    },

    soldStock: {
        type: Number,
        default: 0,
        min: 0,
        validate: Number.isSafeInteger,
    },

    isActive: {
        type: Boolean,
        default: true,
    }
},{
    timestamps:true,
    versionKey:false
})

module.exports=mongoose.model("Product",productSchema);