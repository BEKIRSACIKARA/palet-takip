from flask import Flask, request, jsonify, send_from_directory, send_file
from flask_cors import CORS
import hashlib
import jwt
import datetime
import os
import openpyxl
from openpyxl.styles import Font, PatternFill
from io import BytesIO
from functools import wraps
import psycopg2
import psycopg2.extras
import urllib.parse

app = Flask(__name__, static_folder='static')
app.config['SECRET_KEY'] = 'palet-takip-gizli-anahtar-2026'
CORS(app)

# Palet tipleri (başlangıç)
PALET_TIPLERI = [
    ("P001", "Euro Palet"),
    ("P002", "Sanayi Paleti"),
    ("P003", "Plastik Palet")
]

# Stok sahibi tipleri
SAHIP_TIP_DEPO = "DEPO"
SAHIP_TIP_DAGITICI = "DAGITICI"
SAHIP_TIP_MUSTERI = "MUSTERI"

# Hareket tipleri
HAREKET_DEPO_DAGITICI = "DEPO_DAGITICI"
HAREKET_DAGITICI_MUSTERI = "DAGITICI_MUSTERI"
HAREKET_MUSTERI_DAGITICI = "MUSTERI_DAGITICI"
HAREKET_DAGITICI_DEPO = "DAGITICI_DEPO"
HAREKET_DEPO_STOK = "DEPO_STOK_HAREKET"


def hash_sifre(sifre):
    return hashlib.sha256(sifre.encode()).hexdigest()


def get_db_connection():
    """Veritabanı bağlantısı - PostgreSQL veya SQLite"""
    database_url = os.environ.get('DATABASE_URL')
    if database_url:
        # PostgreSQL (Render)
        urllib.parse.uses_netloc.append('postgres')
        url = urllib.parse.urlparse(database_url)
        conn = psycopg2.connect(
            database=url.path[1:],
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port
        )
        return conn
    else:
        # SQLite (geliştirme için)
        import sqlite3
        conn = sqlite3.connect('palet_takip.db')
        conn.row_factory = sqlite3.Row
        return conn


def veritabani_olustur():
    """Veritabanı tablolarını oluştur"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Kullanıcılar tablosu
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS kullanicilar (
            id SERIAL PRIMARY KEY,
            kullanici_adi TEXT UNIQUE NOT NULL,
            sifre TEXT NOT NULL,
            tip TEXT NOT NULL,
            ad_soyad TEXT NOT NULL
        )
    ''')
    
    # Müşteriler tablosu
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS musteriler (
            id SERIAL PRIMARY KEY,
            musteri_kodu TEXT UNIQUE NOT NULL,
            musteri_adi TEXT NOT NULL,
            tabela_adi TEXT NOT NULL
        )
    ''')
    
    # Palet tipleri tablosu
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS palet_tipleri (
            id SERIAL PRIMARY KEY,
            stok_kodu TEXT UNIQUE NOT NULL,
            palet_adi TEXT NOT NULL
        )
    ''')
    
    # Stoklar tablosu
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stoklar (
            id SERIAL PRIMARY KEY,
            stok_sahibi_tip TEXT NOT NULL,
            stok_sahibi_id INTEGER NOT NULL,
            palet_tipi_id INTEGER NOT NULL,
            miktar INTEGER DEFAULT 0,
            FOREIGN KEY (palet_tipi_id) REFERENCES palet_tipleri(id),
            UNIQUE(stok_sahibi_tip, stok_sahibi_id, palet_tipi_id)
        )
    ''')
    
    # Hareketler tablosu
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS hareketler (
            id SERIAL PRIMARY KEY,
            tarih TEXT NOT NULL,
            yapan_kullanici_id INTEGER NOT NULL,
            hareket_tipi TEXT NOT NULL,
            gonderen_tip TEXT NOT NULL,
            gonderen_id INTEGER NOT NULL,
            alan_tip TEXT NOT NULL,
            alan_id INTEGER NOT NULL,
            palet_tipi_id INTEGER NOT NULL,
            miktar INTEGER NOT NULL,
            aciklama TEXT,
            FOREIGN KEY (yapan_kullanici_id) REFERENCES kullanicilar(id),
            FOREIGN KEY (palet_tipi_id) REFERENCES palet_tipleri(id)
        )
    ''')
    
    conn.commit()
    
    # Palet tiplerini ekle
    for stok_kodu, palet_adi in PALET_TIPLERI:
        cursor.execute('''
            INSERT INTO palet_tipleri (stok_kodu, palet_adi)
            SELECT %s, %s
            WHERE NOT EXISTS (SELECT 1 FROM palet_tipleri WHERE stok_kodu = %s)
        ''', (stok_kodu, palet_adi, stok_kodu))
    
    # Varsayılan depocu
    cursor.execute('''
        INSERT INTO kullanicilar (kullanici_adi, sifre, tip, ad_soyad)
        SELECT %s, %s, %s, %s
        WHERE NOT EXISTS (SELECT 1 FROM kullanicilar WHERE kullanici_adi = %s)
    ''', ('depocu', hash_sifre('1234'), 'DEPOCU', 'Ana Depocu', 'depocu'))
    
    conn.commit()
    
    # Depo stoklarını oluştur
    cursor.execute('SELECT id FROM palet_tipleri')
    palet_ids = cursor.fetchall()
    for palet in palet_ids:
        cursor.execute('''
            INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar)
            SELECT %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM stoklar 
                WHERE stok_sahibi_tip = %s AND stok_sahibi_id = %s AND palet_tipi_id = %s
            )
        ''', ('DEPO', 0, palet[0], 0, 'DEPO', 0, palet[0]))
    
    conn.commit()
    cursor.close()
    conn.close()


def stok_miktari_getir(stok_sahibi_tip, stok_sahibi_id, palet_tipi_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT miktar FROM stoklar
        WHERE stok_sahibi_tip = %s AND stok_sahibi_id = %s AND palet_tipi_id = %s
    ''', (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id))
    sonuc = cursor.fetchone()
    cursor.close()
    conn.close()
    return sonuc[0] if sonuc else 0


def stok_guncelle(stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, degisim):
    conn = get_db_connection()
    cursor = conn.cursor()
    mevcut = stok_miktari_getir(stok_sahibi_tip, stok_sahibi_id, palet_tipi_id)
    yeni_miktar = mevcut + degisim
    if yeni_miktar < 0:
        cursor.close()
        conn.close()
        return False, "Stok yetersiz!"
    cursor.execute('''
        UPDATE stoklar SET miktar = %s
        WHERE stok_sahibi_tip = %s AND stok_sahibi_id = %s AND palet_tipi_id = %s
    ''', (yeni_miktar, stok_sahibi_tip, stok_sahibi_id, palet_tipi_id))
    conn.commit()
    cursor.close()
    conn.close()
    return True, ""


def hareket_kaydet(yapan_kullanici_id, hareket_tipi, gonderen_tip, gonderen_id,
                    alan_tip, alan_id, palet_tipi_id, miktar, aciklama=""):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO hareketler (tarih, yapan_kullanici_id, hareket_tipi,
                                gonderen_tip, gonderen_id, alan_tip, alan_id,
                                palet_tipi_id, miktar, aciklama)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
          yapan_kullanici_id, hareket_tipi, gonderen_tip, gonderen_id,
          alan_tip, alan_id, palet_tipi_id, miktar, aciklama))
    conn.commit()
    cursor.close()
    conn.close()


# Token doğrulama decorator'ı
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'hata': 'Token gerekli'}), 401
        
        try:
            token = token.replace('Bearer ', '')
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            current_user = data
        except:
            return jsonify({'hata': 'Geçersiz token'}), 401
        
        return f(current_user, *args, **kwargs)
    return decorated


# ==================== ANA SAYFA ====================

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')


# ==================== KULLANICI İŞLEMLERİ ====================

@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    kullanici_adi = data.get('kullanici_adi')
    sifre = data.get('sifre')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, kullanici_adi, tip, ad_soyad FROM kullanicilar
        WHERE kullanici_adi = %s AND sifre = %s
    ''', (kullanici_adi, hash_sifre(sifre)))
    kullanici = cursor.fetchone()
    cursor.close()
    conn.close()
    
    if kullanici:
        token = jwt.encode({
            'id': kullanici[0],
            'kullanici_adi': kullanici[1],
            'tip': kullanici[2],
            'ad_soyad': kullanici[3],
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=24)
        }, app.config['SECRET_KEY'], algorithm='HS256')
        
        return jsonify({
            'success': True,
            'token': token,
            'kullanici': {
                'id': kullanici[0],
                'kullanici_adi': kullanici[1],
                'tip': kullanici[2],
                'ad_soyad': kullanici[3]
            }
        })
    else:
        return jsonify({'success': False, 'hata': 'Hatalı kullanıcı adı veya şifre'}), 401


@app.route('/api/kullanici_listesi', methods=['GET'])
@token_required
def get_kullanici_listesi(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, kullanici_adi, ad_soyad FROM kullanicilar
        WHERE tip = 'DAGITICI'
        ORDER BY ad_soyad
    ''')
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    
    kullanicilar = [{'id': k[0], 'kullanici_adi': k[1], 'ad_soyad': k[2]} for k in sonuc]
    return jsonify(kullanicilar)


@app.route('/api/kullanici_duzenle', methods=['PUT'])
@token_required
def kullanici_duzenle(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    data = request.get_json()
    kullanici_id = data.get('id')
    kullanici_adi = data.get('kullanici_adi')
    ad_soyad = data.get('ad_soyad')
    sifre = data.get('sifre')
    
    if not kullanici_id or not kullanici_adi or not ad_soyad:
        return jsonify({'hata': 'ID, kullanıcı adı ve ad soyad gerekli'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if sifre and len(sifre) >= 4:
            cursor.execute('''
                UPDATE kullanicilar 
                SET kullanici_adi = %s, ad_soyad = %s, sifre = %s
                WHERE id = %s AND tip = 'DAGITICI'
            ''', (kullanici_adi, ad_soyad, hash_sifre(sifre), kullanici_id))
        else:
            cursor.execute('''
                UPDATE kullanicilar 
                SET kullanici_adi = %s, ad_soyad = %s
                WHERE id = %s AND tip = 'DAGITICI'
            ''', (kullanici_adi, ad_soyad, kullanici_id))
        
        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            return jsonify({'hata': 'Kullanıcı bulunamadı'}), 404
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'mesaj': 'Kullanıcı güncellendi'})
        
    except Exception as e:
        cursor.close()
        conn.close()
        return jsonify({'hata': str(e)}), 400


@app.route('/api/kullanici_sil', methods=['DELETE'])
@token_required
def kullanici_sil(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    kullanici_id = request.args.get('id', type=int)
    
    if not kullanici_id:
        return jsonify({'hata': 'ID gerekli'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('DELETE FROM stoklar WHERE stok_sahibi_tip = %s AND stok_sahibi_id = %s', 
                   (SAHIP_TIP_DAGITICI, kullanici_id))
    cursor.execute('DELETE FROM kullanicilar WHERE id = %s AND tip = %s', (kullanici_id, 'DAGITICI'))
    
    if cursor.rowcount == 0:
        cursor.close()
        conn.close()
        return jsonify({'hata': 'Kullanıcı bulunamadı'}), 404
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return jsonify({'success': True, 'mesaj': 'Kullanıcı silindi'})


@app.route('/api/dagitici_ekle', methods=['POST'])
@token_required
def dagitici_ekle(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    data = request.get_json()
    kullanici_adi = data.get('kullanici_adi')
    ad_soyad = data.get('ad_soyad')
    sifre = data.get('sifre')
    
    if not kullanici_adi or not ad_soyad or not sifre:
        return jsonify({'hata': 'Tüm alanlar gerekli'}), 400
    
    if len(sifre) < 4:
        return jsonify({'hata': 'Şifre en az 4 karakter olmalı'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO kullanicilar (kullanici_adi, sifre, tip, ad_soyad)
            VALUES (%s, %s, %s, %s)
        ''', (kullanici_adi, hash_sifre(sifre), 'DAGITICI', ad_soyad))
        
        dagitici_id = cursor.lastrowid
        
        cursor.execute("SELECT id FROM palet_tipleri")
        paletler = cursor.fetchall()
        for palet in paletler:
            cursor.execute('''
                INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar)
                VALUES (%s, %s, %s, 0)
            ''', (SAHIP_TIP_DAGITICI, dagitici_id, palet[0]))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'id': dagitici_id, 'mesaj': 'Dağıtıcı eklendi'})
        
    except Exception as e:
        cursor.close()
        conn.close()
        return jsonify({'hata': str(e)}), 400


# ==================== MÜŞTERİ İŞLEMLERİ ====================

@app.route('/api/musteri_ekle', methods=['POST'])
@token_required
def musteri_ekle(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    data = request.get_json()
    musteri_kodu = data.get('musteri_kodu')
    musteri_adi = data.get('musteri_adi')
    tabela_adi = data.get('tabela_adi')
    
    if not musteri_kodu or not musteri_adi or not tabela_adi:
        return jsonify({'hata': 'Tüm alanlar gerekli'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO musteriler (musteri_kodu, musteri_adi, tabela_adi)
            VALUES (%s, %s, %s)
        ''', (musteri_kodu, musteri_adi, tabela_adi))
        
        musteri_id = cursor.lastrowid
        
        cursor.execute("SELECT id FROM palet_tipleri")
        paletler = cursor.fetchall()
        for palet in paletler:
            cursor.execute('''
                INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar)
                VALUES (%s, %s, %s, 0)
            ''', (SAHIP_TIP_MUSTERI, musteri_id, palet[0]))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'id': musteri_id, 'mesaj': 'Müşteri eklendi'})
        
    except Exception as e:
        cursor.close()
        conn.close()
        return jsonify({'hata': str(e)}), 400


@app.route('/api/tum_musteriler', methods=['GET'])
@token_required
def get_tum_musteriler(current_user):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, musteri_kodu, musteri_adi, tabela_adi FROM musteriler
        ORDER BY musteri_adi
    ''')
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    
    musteriler = [{'id': m[0], 'musteri_kodu': m[1], 'musteri_adi': m[2], 'tabela_adi': m[3]} for m in sonuc]
    return jsonify(musteriler)


@app.route('/api/musteri_sil', methods=['DELETE'])
@token_required
def musteri_sil(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    musteri_id = request.args.get('id', type=int)
    
    if not musteri_id:
        return jsonify({'hata': 'ID gerekli'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT SUM(miktar) FROM stoklar WHERE stok_sahibi_tip = %s AND stok_sahibi_id = %s', 
                   (SAHIP_TIP_MUSTERI, musteri_id))
    toplam_stok = cursor.fetchone()[0]
    
    if toplam_stok and toplam_stok > 0:
        cursor.close()
        conn.close()
        return jsonify({'hata': f'Bu müşterinin {toplam_stok} adet paleti var. Önce stokları boşaltın!'}), 400
    
    cursor.execute('DELETE FROM stoklar WHERE stok_sahibi_tip = %s AND stok_sahibi_id = %s', 
                   (SAHIP_TIP_MUSTERI, musteri_id))
    cursor.execute('DELETE FROM musteriler WHERE id = %s', (musteri_id,))
    
    if cursor.rowcount == 0:
        cursor.close()
        conn.close()
        return jsonify({'hata': 'Müşteri bulunamadı'}), 404
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return jsonify({'success': True, 'mesaj': 'Müşteri silindi'})


# ==================== PALET TİPLERİ ====================

@app.route('/api/palet_tipleri', methods=['GET'])
@token_required
def get_palet_tipleri(current_user):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, stok_kodu, palet_adi FROM palet_tipleri")
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    
    paletler = [{'id': p[0], 'stok_kodu': p[1], 'palet_adi': p[2]} for p in sonuc]
    return jsonify(paletler)


@app.route('/api/palet_tipi_ekle', methods=['POST'])
@token_required
def palet_tipi_ekle(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    data = request.get_json()
    stok_kodu = data.get('stok_kodu')
    palet_adi = data.get('palet_adi')
    
    if not stok_kodu or not palet_adi:
        return jsonify({'hata': 'Stok kodu ve palet adı gerekli'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO palet_tipleri (stok_kodu, palet_adi)
            VALUES (%s, %s)
        ''', (stok_kodu, palet_adi))
        
        palet_tipi_id = cursor.lastrowid
        
        cursor.execute('''
            INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar)
            VALUES (%s, %s, %s, 0)
        ''', (SAHIP_TIP_DEPO, 0, palet_tipi_id))
        
        cursor.execute('SELECT id FROM kullanicilar WHERE tip = %s', ('DAGITICI',))
        dagiticilar = cursor.fetchall()
        for d in dagiticilar:
            cursor.execute('''
                INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar)
                VALUES (%s, %s, %s, 0)
            ''', (SAHIP_TIP_DAGITICI, d[0], palet_tipi_id))
        
        cursor.execute('SELECT id FROM musteriler')
        musteriler = cursor.fetchall()
        for m in musteriler:
            cursor.execute('''
                INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar)
                VALUES (%s, %s, %s, 0)
            ''', (SAHIP_TIP_MUSTERI, m[0], palet_tipi_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'id': palet_tipi_id, 'mesaj': 'Palet tipi eklendi'})
        
    except Exception as e:
        cursor.close()
        conn.close()
        return jsonify({'hata': str(e)}), 400


@app.route('/api/palet_tipi_duzenle', methods=['PUT'])
@token_required
def palet_tipi_duzenle(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    data = request.get_json()
    palet_tipi_id = data.get('id')
    stok_kodu = data.get('stok_kodu')
    palet_adi = data.get('palet_adi')
    
    if not palet_tipi_id or not stok_kodu or not palet_adi:
        return jsonify({'hata': 'ID, stok kodu ve palet adı gerekli'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            UPDATE palet_tipleri 
            SET stok_kodu = %s, palet_adi = %s
            WHERE id = %s
        ''', (stok_kodu, palet_adi, palet_tipi_id))
        
        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            return jsonify({'hata': 'Palet tipi bulunamadı'}), 404
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'mesaj': 'Palet tipi güncellendi'})
        
    except Exception as e:
        cursor.close()
        conn.close()
        return jsonify({'hata': str(e)}), 400


@app.route('/api/palet_tipi_sil', methods=['DELETE'])
@token_required
def palet_tipi_sil(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    palet_tipi_id = request.args.get('id', type=int)
    
    if not palet_tipi_id:
        return jsonify({'hata': 'ID gerekli'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT SUM(miktar) FROM stoklar 
        WHERE palet_tipi_id = %s AND miktar > 0
    ''', (palet_tipi_id,))
    toplam = cursor.fetchone()[0]
    
    if toplam and toplam > 0:
        cursor.close()
        conn.close()
        return jsonify({'hata': f'Bu palet tipine ait {toplam} adet stok var. Önce stokları boşaltın!'}), 400
    
    try:
        cursor.execute('DELETE FROM stoklar WHERE palet_tipi_id = %s', (palet_tipi_id,))
        cursor.execute('DELETE FROM palet_tipleri WHERE id = %s', (palet_tipi_id,))
        
        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            return jsonify({'hata': 'Palet tipi bulunamadı'}), 404
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({'success': True, 'mesaj': 'Palet tipi silindi'})
        
    except Exception as e:
        cursor.close()
        conn.close()
        return jsonify({'hata': str(e)}), 400


# ==================== STOK İŞLEMLERİ ====================

@app.route('/api/stok', methods=['GET'])
@token_required
def get_stok(current_user):
    tip = request.args.get('tip')
    kimlik = request.args.get('id', type=int)
    
    if not tip or kimlik is None:
        return jsonify({'hata': 'tip ve id parametreleri gerekli'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT pt.id, pt.stok_kodu, pt.palet_adi, COALESCE(s.miktar, 0) as miktar
        FROM palet_tipleri pt
        LEFT JOIN stoklar s ON pt.id = s.palet_tipi_id 
            AND s.stok_sahibi_tip = %s AND s.stok_sahibi_id = %s
        ORDER BY pt.id
    ''', (tip, kimlik))
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    
    stoklar = []
    for p in sonuc:
        stoklar.append({
            'palet_id': p[0],
            'stok_kodu': p[1],
            'palet_adi': p[2],
            'miktar': p[3] if p[3] else 0
        })
    
    return jsonify(stoklar)


@app.route('/api/transfer', methods=['POST'])
@token_required
def transfer_yap(current_user):
    data = request.get_json()
    hareket_tipi = data.get('hareket_tipi')
    palet_tipi_id = data.get('palet_tipi_id')
    miktar = data.get('miktar')
    alici_id = data.get('alici_id')
    
    if not hareket_tipi or not palet_tipi_id or not miktar:
        return jsonify({'hata': 'Eksik parametreler'}), 400
    
    if miktar <= 0:
        return jsonify({'hata': 'Miktar pozitif olmalı'}), 400
    
    kullanici_id = current_user['id']
    kullanici_tip = current_user['tip']
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT stok_kodu, palet_adi FROM palet_tipleri WHERE id = %s", (palet_tipi_id,))
    palet = cursor.fetchone()
    if not palet:
        cursor.close()
        conn.close()
        return jsonify({'hata': 'Geçersiz palet tipi'}), 400
    
    if kullanici_tip == 'DEPOCU':
        if hareket_tipi == 'DEPO_DAGITICI':
            gonderen_tip = SAHIP_TIP_DEPO
            gonderen_id = 0
            alan_tip = SAHIP_TIP_DAGITICI
            alan_id = alici_id
            
            cursor.execute("SELECT id FROM kullanicilar WHERE id = %s AND tip = 'DAGITICI'", (alici_id,))
            if not cursor.fetchone():
                cursor.close()
                conn.close()
                return jsonify({'hata': 'Geçersiz dağıtıcı ID'}), 400
                
        elif hareket_tipi == 'DAGITICI_DEPO':
            gonderen_tip = SAHIP_TIP_DAGITICI
            gonderen_id = alici_id
            alan_tip = SAHIP_TIP_DEPO
            alan_id = 0
            
            cursor.execute("SELECT id FROM kullanicilar WHERE id = %s AND tip = 'DAGITICI'", (alici_id,))
            if not cursor.fetchone():
                cursor.close()
                conn.close()
                return jsonify({'hata': 'Geçersiz dağıtıcı ID'}), 400
        else:
            cursor.close()
            conn.close()
            return jsonify({'hata': 'Geçersiz hareket tipi'}), 400
            
    elif kullanici_tip == 'DAGITICI':
        if hareket_tipi == 'DAGITICI_MUSTERI':
            gonderen_tip = SAHIP_TIP_DAGITICI
            gonderen_id = kullanici_id
            alan_tip = SAHIP_TIP_MUSTERI
            alan_id = alici_id
            
            cursor.execute('SELECT id FROM musteriler WHERE id = %s', (alici_id,))
            if not cursor.fetchone():
                cursor.close()
                conn.close()
                return jsonify({'hata': 'Geçersiz müşteri ID'}), 400
                
        elif hareket_tipi == 'MUSTERI_DAGITICI':
            gonderen_tip = SAHIP_TIP_MUSTERI
            gonderen_id = alici_id
            alan_tip = SAHIP_TIP_DAGITICI
            alan_id = kullanici_id
            
            cursor.execute('SELECT id FROM musteriler WHERE id = %s', (alici_id,))
            if not cursor.fetchone():
                cursor.close()
                conn.close()
                return jsonify({'hata': 'Geçersiz müşteri ID'}), 400
                
        elif hareket_tipi == 'DAGITICI_DEPO':
            gonderen_tip = SAHIP_TIP_DAGITICI
            gonderen_id = kullanici_id
            alan_tip = SAHIP_TIP_DEPO
            alan_id = 0
        else:
            cursor.close()
            conn.close()
            return jsonify({'hata': 'Geçersiz hareket tipi'}), 400
    else:
        cursor.close()
        conn.close()
        return jsonify({'hata': 'Yetkisiz kullanıcı'}), 403
    
    mevcut = stok_miktari_getir(gonderen_tip, gonderen_id, palet_tipi_id)
    if mevcut < miktar:
        cursor.close()
        conn.close()
        return jsonify({'hata': f'Yetersiz stok! Mevcut: {mevcut}'}), 400
    
    basarili, hata = stok_guncelle(gonderen_tip, gonderen_id, palet_tipi_id, -miktar)
    if not basarili:
        cursor.close()
        conn.close()
        return jsonify({'hata': hata}), 400
    
    basarili, hata = stok_guncelle(alan_tip, alan_id, palet_tipi_id, +miktar)
    if not basarili:
        stok_guncelle(gonderen_tip, gonderen_id, palet_tipi_id, +miktar)
        cursor.close()
        conn.close()
        return jsonify({'hata': hata}), 400
    
    aciklama = f"{palet[1]} - {miktar} adet transfer"
    hareket_kaydet(kullanici_id, hareket_tipi, gonderen_tip, gonderen_id,
                   alan_tip, alan_id, palet_tipi_id, miktar, aciklama)
    
    cursor.close()
    conn.close()
    return jsonify({'success': True, 'mesaj': 'Transfer başarılı'})


@app.route('/api/dagitici_listesi', methods=['GET'])
@token_required
def get_dagitici_listesi(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, kullanici_adi, ad_soyad FROM kullanicilar
        WHERE tip = 'DAGITICI'
        ORDER BY ad_soyad
    ''')
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    
    dagiticilar = [{'id': d[0], 'kullanici_adi': d[1], 'ad_soyad': d[2]} for d in sonuc]
    return jsonify(dagiticilar)


# ==================== HAREKETLER ====================

@app.route('/api/hareketler', methods=['GET'])
@token_required
def get_hareketler(current_user):
    limit = request.args.get('limit', 50, type=int)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if current_user['tip'] == 'DEPOCU':
        cursor.execute('''
            SELECT h.tarih, u.kullanici_adi, u.ad_soyad, h.hareket_tipi,
                   pt.stok_kodu, pt.palet_adi, h.miktar, h.aciklama
            FROM hareketler h
            JOIN kullanicilar u ON h.yapan_kullanici_id = u.id
            JOIN palet_tipleri pt ON h.palet_tipi_id = pt.id
            ORDER BY h.tarih DESC
            LIMIT %s
        ''', (limit,))
    else:
        cursor.execute('''
            SELECT h.tarih, u.kullanici_adi, u.ad_soyad, h.hareket_tipi,
                   pt.stok_kodu, pt.palet_adi, h.miktar, h.aciklama
            FROM hareketler h
            JOIN kullanicilar u ON h.yapan_kullanici_id = u.id
            JOIN palet_tipleri pt ON h.palet_tipi_id = pt.id
            WHERE h.gonderen_id = %s OR h.alan_id = %s
            ORDER BY h.tarih DESC
            LIMIT %s
        ''', (current_user['id'], current_user['id'], limit))
    
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    
    hareketler = []
    for h in sonuc:
        tip_text = {
            'DEPO_DAGITICI': 'Depo→Dağıtıcı',
            'DAGITICI_MUSTERI': 'Dağıtıcı→Müşteri',
            'MUSTERI_DAGITICI': 'Müşteri→Dağıtıcı',
            'DAGITICI_DEPO': 'Dağıtıcı→Depo',
            'DEPO_STOK_HAREKET': 'Depo Stok Hareketi'
        }.get(h[3], h[3])
        
        hareketler.append({
            'tarih': h[0],
            'yapan': f"{h[2]} ({h[1]})",
            'islem_tipi': tip_text,
            'stok_kodu': h[4],
            'palet_adi': h[5],
            'miktar': h[6],
            'aciklama': h[7]
        })
    
    return jsonify(hareketler)


# ==================== DEPO STOK HAREKETLERİ ====================

@app.route('/api/depo_stok_hareket', methods=['POST'])
@token_required
def depo_stok_hareket(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    data = request.get_json()
    palet_tipi_id = data.get('palet_tipi_id')
    miktar = data.get('miktar')
    islem_tipi = data.get('islem_tipi')
    aciklama = data.get('aciklama', '')
    
    if not palet_tipi_id or not miktar or not islem_tipi:
        return jsonify({'hata': 'Eksik parametreler'}), 400
    
    if miktar <= 0:
        return jsonify({'hata': 'Miktar pozitif olmalı'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT stok_kodu, palet_adi FROM palet_tipleri WHERE id = %s", (palet_tipi_id,))
    palet = cursor.fetchone()
    if not palet:
        cursor.close()
        conn.close()
        return jsonify({'hata': 'Geçersiz palet tipi'}), 400
    
    mevcut = stok_miktari_getir(SAHIP_TIP_DEPO, 0, palet_tipi_id)
    
    if islem_tipi == 'azalt':
        if mevcut < miktar:
            cursor.close()
            conn.close()
            return jsonify({'hata': f'Yetersiz stok! Mevcut: {mevcut}'}), 400
        yeni_miktar = mevcut - miktar
        degisim = -miktar
        hareket_aciklama = f"STOK AZALTMA: {miktar} adet {palet[1]} azaltıldı. Sebep: {aciklama}"
    else:
        yeni_miktar = mevcut + miktar
        degisim = +miktar
        hareket_aciklama = f"STOK ARTTIRMA: {miktar} adet {palet[1]} eklendi. Sebep: {aciklama}"
    
    basarili, hata = stok_guncelle(SAHIP_TIP_DEPO, 0, palet_tipi_id, degisim)
    if not basarili:
        cursor.close()
        conn.close()
        return jsonify({'hata': hata}), 400
    
    hareket_kaydet(
        current_user['id'],
        "DEPO_STOK_HAREKET",
        SAHIP_TIP_DEPO, 0,
        SAHIP_TIP_DEPO, 0,
        palet_tipi_id, miktar,
        hareket_aciklama
    )
    
    cursor.close()
    conn.close()
    
    return jsonify({
        'success': True,
        'mesaj': f'{palet[1]} stoğu güncellendi. Yeni miktar: {yeni_miktar}',
        'yeni_miktar': yeni_miktar
    })


# ==================== RAPORLAR VE İSTATİSTİKLER ====================

@app.route('/api/rapor/hareketler', methods=['POST'])
@token_required
def rapor_hareketler(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    data = request.get_json()
    baslangic = data.get('baslangic_tarihi')
    bitis = data.get('bitis_tarihi')
    
    if not baslangic or not bitis:
        return jsonify({'hata': 'Başlangıç ve bitiş tarihi gerekli'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT h.tarih, u.kullanici_adi, u.ad_soyad, h.hareket_tipi,
               pt.stok_kodu, pt.palet_adi, h.miktar, h.aciklama
        FROM hareketler h
        JOIN kullanicilar u ON h.yapan_kullanici_id = u.id
        JOIN palet_tipleri pt ON h.palet_tipi_id = pt.id
        WHERE DATE(h.tarih) BETWEEN %s AND %s
        ORDER BY h.tarih DESC
    ''', (baslangic, bitis))
    
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    
    hareketler = []
    for h in sonuc:
        tip_text = {
            'DEPO_DAGITICI': 'Depo→Dağıtıcı',
            'DAGITICI_MUSTERI': 'Dağıtıcı→Müşteri',
            'MUSTERI_DAGITICI': 'Müşteri→Dağıtıcı',
            'DAGITICI_DEPO': 'Dağıtıcı→Depo',
            'DEPO_STOK_HAREKET': 'Depo Stok Hareketi'
        }.get(h[3], h[3])
        
        hareketler.append({
            'tarih': h[0],
            'yapan': f"{h[2]} ({h[1]})",
            'islem_tipi': tip_text,
            'stok_kodu': h[4],
            'palet_adi': h[5],
            'miktar': h[6],
            'aciklama': h[7]
        })
    
    return jsonify(hareketler)


@app.route('/api/rapor/istatistikler', methods=['GET'])
@token_required
def rapor_istatistikler(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # En çok transfer yapan dağıtıcılar
    cursor.execute('''
        SELECT u.kullanici_adi, u.ad_soyad, COUNT(*) as transfer_sayisi
        FROM hareketler h
        JOIN kullanicilar u ON h.yapan_kullanici_id = u.id
        WHERE u.tip = 'DAGITICI'
        GROUP BY u.id
        ORDER BY transfer_sayisi DESC
        LIMIT 10
    ''')
    en_cok_transfer = [{'kullanici_adi': r[0], 'ad_soyad': r[1], 'transfer_sayisi': r[2]} for r in cursor.fetchall()]
    
    # En çok kullanılan palet tipi
    cursor.execute('''
        SELECT pt.stok_kodu, pt.palet_adi, COUNT(*) as kullanim_sayisi, SUM(h.miktar) as toplam_miktar
        FROM hareketler h
        JOIN palet_tipleri pt ON h.palet_tipi_id = pt.id
        WHERE h.hareket_tipi != 'DEPO_STOK_HAREKET'
        GROUP BY pt.id
        ORDER BY kullanim_sayisi DESC
    ''')
    en_cok_palet = [{'stok_kodu': r[0], 'palet_adi': r[1], 'kullanim_sayisi': r[2], 'toplam_miktar': r[3]} for r in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    return jsonify({
        'en_cok_transfer_yapan': en_cok_transfer,
        'en_cok_kullanilan_palet': en_cok_palet
    })


# ==================== EXCEL EXPORT ====================

@app.route('/api/rapor/export', methods=['POST'])
@token_required
def rapor_export(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    data = request.get_json()
    rapor_tipi = data.get('rapor_tipi')
    baslangic = data.get('baslangic_tarihi')
    bitis = data.get('bitis_tarihi')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    workbook = openpyxl.Workbook()
    
    if rapor_tipi == 'hareketler':
        sheet = workbook.active
        sheet.title = "Hareketler"
        
        headers = ['Tarih', 'Yapan Kullanıcı', 'İşlem Tipi', 'Stok Kodu', 'Palet Adı', 'Miktar', 'Açıklama']
        for col, header in enumerate(headers, 1):
            cell = sheet.cell(row=1, column=col, value=header)
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill(start_color="2196F3", end_color="2196F3", fill_type="solid")
        
        if baslangic and bitis:
            cursor.execute('''
                SELECT h.tarih, u.ad_soyad, h.hareket_tipi, pt.stok_kodu, pt.palet_adi, h.miktar, h.aciklama
                FROM hareketler h
                JOIN kullanicilar u ON h.yapan_kullanici_id = u.id
                JOIN palet_tipleri pt ON h.palet_tipi_id = pt.id
                WHERE DATE(h.tarih) BETWEEN %s AND %s
                ORDER BY h.tarih DESC
            ''', (baslangic, bitis))
        else:
            cursor.execute('''
                SELECT h.tarih, u.ad_soyad, h.hareket_tipi, pt.stok_kodu, pt.palet_adi, h.miktar, h.aciklama
                FROM hareketler h
                JOIN kullanicilar u ON h.yapan_kullanici_id = u.id
                JOIN palet_tipleri pt ON h.palet_tipi_id = pt.id
                ORDER BY h.tarih DESC
                LIMIT 1000
            ''')
        
        for row_idx, row in enumerate(cursor.fetchall(), 2):
            tip_text = {
                'DEPO_DAGITICI': 'Depo→Dağıtıcı',
                'DAGITICI_MUSTERI': 'Dağıtıcı→Müşteri',
                'MUSTERI_DAGITICI': 'Müşteri→Dağıtıcı',
                'DAGITICI_DEPO': 'Dağıtıcı→Depo',
                'DEPO_STOK_HAREKET': 'Depo Stok Hareketi'
            }.get(row[2], row[2])
            
            sheet.cell(row=row_idx, column=1, value=row[0])
            sheet.cell(row=row_idx, column=2, value=row[1])
            sheet.cell(row=row_idx, column=3, value=tip_text)
            sheet.cell(row=row_idx, column=4, value=row[3])
            sheet.cell(row=row_idx, column=5, value=row[4])
            sheet.cell(row=row_idx, column=6, value=row[5])
            sheet.cell(row=row_idx, column=7, value=row[6])
        
        for col in range(1, 8):
            sheet.column_dimensions[openpyxl.utils.get_column_letter(col)].width = 20
            
    elif rapor_tipi == 'stoklar':
        sheet = workbook.active
        sheet.title = "Stoklar"
        
        headers = ['Stok Sahibi', 'Stok Kodu', 'Palet Adı', 'Miktar']
        for col, header in enumerate(headers, 1):
            cell = sheet.cell(row=1, column=col, value=header)
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill(start_color="2196F3", end_color="2196F3", fill_type="solid")
        
        row_idx = 2
        cursor.execute('''
            SELECT pt.stok_kodu, pt.palet_adi, s.miktar
            FROM stoklar s
            JOIN palet_tipleri pt ON s.palet_tipi_id = pt.id
            WHERE s.stok_sahibi_tip = 'DEPO'
        ''')
        for row in cursor.fetchall():
            sheet.cell(row=row_idx, column=1, value="DEPO")
            sheet.cell(row=row_idx, column=2, value=row[0])
            sheet.cell(row=row_idx, column=3, value=row[1])
            sheet.cell(row=row_idx, column=4, value=row[2])
            row_idx += 1
        
        cursor.execute('''
            SELECT u.ad_soyad, pt.stok_kodu, pt.palet_adi, s.miktar
            FROM stoklar s
            JOIN kullanicilar u ON s.stok_sahibi_id = u.id
            JOIN palet_tipleri pt ON s.palet_tipi_id = pt.id
            WHERE s.stok_sahibi_tip = 'DAGITICI' AND s.miktar > 0
            ORDER BY u.ad_soyad
        ''')
        for row in cursor.fetchall():
            sheet.cell(row=row_idx, column=1, value=row[0])
            sheet.cell(row=row_idx, column=2, value=row[1])
            sheet.cell(row=row_idx, column=3, value=row[2])
            sheet.cell(row=row_idx, column=4, value=row[3])
            row_idx += 1
        
        cursor.execute('''
            SELECT m.musteri_kodu, m.musteri_adi, pt.stok_kodu, pt.palet_adi, s.miktar
            FROM stoklar s
            JOIN musteriler m ON s.stok_sahibi_id = m.id
            JOIN palet_tipleri pt ON s.palet_tipi_id = pt.id
            WHERE s.stok_sahibi_tip = 'MUSTERI' AND s.miktar > 0
            ORDER BY m.musteri_adi
        ''')
        for row in cursor.fetchall():
            sheet.cell(row=row_idx, column=1, value=f"{row[0]} - {row[1]}")
            sheet.cell(row=row_idx, column=2, value=row[2])
            sheet.cell(row=row_idx, column=3, value=row[3])
            sheet.cell(row=row_idx, column=4, value=row[4])
            row_idx += 1
        
        for col in range(1, 5):
            sheet.column_dimensions[openpyxl.utils.get_column_letter(col)].width = 25
            
    elif rapor_tipi == 'musteriler':
        sheet = workbook.active
        sheet.title = "Müşteriler"
        
        headers = ['Müşteri Kodu', 'Müşteri Adı', 'Tabela Adı']
        for col, header in enumerate(headers, 1):
            cell = sheet.cell(row=1, column=col, value=header)
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill(start_color="2196F3", end_color="2196F3", fill_type="solid")
        
        cursor.execute('SELECT musteri_kodu, musteri_adi, tabela_adi FROM musteriler ORDER BY musteri_adi')
        for row_idx, row in enumerate(cursor.fetchall(), 2):
            sheet.cell(row=row_idx, column=1, value=row[0])
            sheet.cell(row=row_idx, column=2, value=row[1])
            sheet.cell(row=row_idx, column=3, value=row[2])
        
        for col in range(1, 4):
            sheet.column_dimensions[openpyxl.utils.get_column_letter(col)].width = 25
    
    cursor.close()
    conn.close()
    
    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'{rapor_tipi}_raporu.xlsx'
    )


# ==================== EXCEL YÜKLEME ====================

@app.route('/api/musteri_excel_yukle', methods=['POST'])
@token_required
def musteri_excel_yukle(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    
    if 'file' not in request.files:
        return jsonify({'hata': 'Dosya bulunamadı'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'hata': 'Dosya seçilmedi'}), 400
    
    try:
        workbook = openpyxl.load_workbook(file)
        sheet = workbook.active
    except Exception as e:
        return jsonify({'hata': f'Excel okunamadı: {str(e)}'}), 400
    
    headers = []
    for col in range(1, sheet.max_column + 1):
        val = sheet.cell(row=1, column=col).value
        headers.append(str(val).strip().upper() if val else '')
    
    required_columns = ['MÜŞTERİ KODU', 'MÜŞTERİ ADI', 'TABELA ADI']
    for col in required_columns:
        if col not in headers:
            return jsonify({'hata': f"'{col}' sütunu bulunamadı. Sütunlar: {headers}"}), 400
    
    col_indices = {h: i+1 for i, h in enumerate(headers)}
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT musteri_kodu FROM musteriler')
    mevcut_kodlar = {k[0] for k in cursor.fetchall()}
    
    eklenen = 0
    guncellenen = 0
    hatalar = []
    
    for row in range(2, sheet.max_row + 1):
        musteri_kodu_cell = sheet.cell(row=row, column=col_indices['MÜŞTERİ KODU']).value
        musteri_adi_cell = sheet.cell(row=row, column=col_indices['MÜŞTERİ ADI']).value
        tabela_adi_cell = sheet.cell(row=row, column=col_indices['TABELA ADI']).value
        
        musteri_kodu = str(musteri_kodu_cell).strip() if musteri_kodu_cell else ''
        musteri_adi = str(musteri_adi_cell).strip() if musteri_adi_cell else ''
        tabela_adi = str(tabela_adi_cell).strip() if tabela_adi_cell else ''
        
        if not musteri_kodu or not musteri_adi or not tabela_adi:
            hatalar.append(f"Satır {row}: Boş alan var")
            continue
        
        if musteri_kodu in mevcut_kodlar:
            try:
                cursor.execute('''
                    UPDATE musteriler 
                    SET musteri_adi = %s, tabela_adi = %s
                    WHERE musteri_kodu = %s
                ''', (musteri_adi, tabela_adi, musteri_kodu))
                guncellenen += 1
            except Exception as e:
                hatalar.append(f"Satır {row}: Güncelleme hatası - {str(e)}")
        else:
            try:
                cursor.execute('''
                    INSERT INTO musteriler (musteri_kodu, musteri_adi, tabela_adi)
                    VALUES (%s, %s, %s)
                ''', (musteri_kodu, musteri_adi, tabela_adi))
                
                musteri_id = cursor.lastrowid
                
                cursor.execute("SELECT id FROM palet_tipleri")
                paletler = cursor.fetchall()
                for palet in paletler:
                    cursor.execute('''
                        INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar)
                        VALUES (%s, %s, %s, 0)
                    ''', (SAHIP_TIP_MUSTERI, musteri_id, palet[0]))
                
                eklenen += 1
                mevcut_kodlar.add(musteri_kodu)
                
            except Exception as e:
                hatalar.append(f"Satır {row}: Ekleme hatası - {str(e)}")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    mesaj = f'{eklenen} yeni müşteri eklendi, {guncellenen} müşteri güncellendi.'
    if hatalar:
        mesaj += f' {len(hatalar)} hata oluştu.'
    
    return jsonify({
        'success': True,
        'eklenen': eklenen,
        'guncellenen': guncellenen,
        'hatalar': hatalar,
        'mesaj': mesaj
    })


# ==================== UYGULAMA BAŞLATMA ====================

if __name__ == '__main__':
    veritabani_olustur()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
